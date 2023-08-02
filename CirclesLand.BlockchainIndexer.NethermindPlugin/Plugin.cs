using CirclesLand.BlockchainIndexer.Persistence;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;
using Nethermind.Api;
using Nethermind.Api.Extensions;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Extensions;
using Npgsql;
using Block = Nethermind.Core.Block;
using Transaction = Nethermind.Core.Transaction;

namespace CirclesLand.BlockchainIndexer.NethermindPlugin;

record TransactionWithReceipt(Block Block, Transaction Transaction, TxReceipt? Receipt);

record TransactionWithEvents(TransactionWithReceipt TransactionWithReceipts,
    IList<(TransactionClass, IDetail?, Exception?)> Events);

public class Plugin : INethermindPlugin
{
    public string Name => "CirclesLand.BlockchainIndexer.NethermindPlugin";
    public string Description => "CirclesLand.BlockchainIndexer.NethermindPlugin";
    public string Author => "Gnosis Service GmbH";

    public async Task Init(INethermindApi nethermindApi)
    {
        long from = 0;

        Task.Run(async () =>
        {
            while (true)
            {
                long? to = nethermindApi.BlockTree?.Head?.Number;
                string connectionString = "postgres://postgres:postgres@localhost:5432/index";
                SslMode sslMode = SslMode.Prefer;

                while (to == null || to == 0)
                {
                    Console.WriteLine("Waiting for the block tree to sync...");
                    await Task.Delay(1000);
                    to = nethermindApi.BlockTree?.Head?.Number;
                }

                IEnumerable<Block> blocks = DownloadBlocks(nethermindApi, from, to.Value);
                IEnumerable<TransactionWithReceipt> transactionsWithReceipt = DownloadReceipts(nethermindApi, blocks);
                IEnumerable<TransactionWithEvents> transactionsWithEvents = ExtractEvents(transactionsWithReceipt);

                await Insert(connectionString, sslMode, true, transactionsWithEvents);

                from = to.Value;

                Console.WriteLine("All done. Waiting for the next blocks...");
                await Task.Delay(1000);
            }
        });
    }

    private IEnumerable<Block> DownloadBlocks(INethermindApi nethermindApi, long from, long to)
    {
        Block GetBlock(long blockNumber)
        {
            Block? block = nethermindApi.BlockTree?.FindBlock(blockNumber);
            if (block == null)
            {
                throw new InvalidOperationException(
                    $"Requested block {blockNumber} from the BlockTree but couldn't find it.");
            }

            return block;
        }

        for (long blockNumber = from; blockNumber <= to; blockNumber++)
        {
            Block block = GetBlock(blockNumber);
            yield return block;
        }
    }

    private IEnumerable<TransactionWithReceipt> DownloadReceipts(INethermindApi nethermindApi,
        IEnumerable<Block> blocks)
    {
        IEnumerable<TxReceipt> Download(Block block)
        {
            Console.WriteLine($"Downloading receipts");

            Dictionary<Keccak, TxReceipt> receipts = (nethermindApi.ReceiptFinder?.Get(block)
                                                      ?? Array.Empty<TxReceipt>())
                .ToDictionary(o => o.TxHash!);

            foreach (var transaction in block.Transactions)
            {
                Console.WriteLine($"* Downloading receipt for transaction {transaction.Hash}...");

                if (!receipts.TryGetValue(transaction.Hash!, out var receipt))
                {
                    Console.WriteLine($"  Warning: Receipt for transaction {transaction.Hash} not found");
                    continue;
                }

                yield return receipt;
            }

            Console.WriteLine();
        }

        foreach (var block in blocks)
        {
            var receipts = Download(block)
                .ToDictionary(o => o.TxHash!);

            foreach (var transaction in block.Transactions)
            {
                if (!receipts.TryGetValue(transaction.Hash!, out var receipt))
                {
                    Console.WriteLine($"  Warning: Receipt for transaction {transaction.Hash} not found");
                    yield return new TransactionWithReceipt(block, transaction, null);
                }
                else
                {
                    yield return new TransactionWithReceipt(block, transaction, receipt);
                }
            }
        }
    }

    private static IEnumerable<TransactionWithEvents> ExtractEvents(
        IEnumerable<TransactionWithReceipt> transactionsWithReceipt)
    {
        foreach (var transactionWithReceipt in transactionsWithReceipt)
        {
            var transaction = transactionWithReceipt.Transaction;
            var receipt = transactionWithReceipt.Receipt;
            var events = new List<(TransactionClass, IDetail?, Exception?)>();

            if (receipt != null)
            {
                foreach (var log in receipt.Logs)
                {
                    Extractors.ExtractCrcTrust(log, events);
                    Extractors.ExtractCrcSignup(receipt, events);
                    Extractors.ExtractCrcHubTransfer(receipt, events);
                    Extractors.ExtractCrcOrganisationSignup(log, events);
                    Extractors.ExtractErc20Transfer(log, events);
                }

                Extractors.ExtractEoaEthTransfer(transaction, receipt, events);
            }
            else
            {
                Console.WriteLine($"  Warning: Receipt for transaction {transaction.Hash} not found");
            }

            yield return new TransactionWithEvents(transactionWithReceipt, events);
        }
    }

    private async Task Insert(string connectionString, SslMode sslMode, bool trustServerCertificate,
        IEnumerable<TransactionWithEvents> transactionsWithEvents)
    {
        var pgsqlConnection =
            new NpgsqlConnection(ConvertDatabaseUrlToConnectionString(connectionString, SslMode.Prefer, true));
        await pgsqlConnection.OpenAsync();

        long lastBlockNo = -1;

        await Materialize(transactionsWithEvents, async transactionWithEvents =>
        {
            var transaction = transactionWithEvents.TransactionWithReceipts.Transaction;
            var receipt = transactionWithEvents.TransactionWithReceipts.Receipt;
            var events = transactionWithEvents.Events
                .Where(o => o.Item2 != null)
                .Select(o => o.Item2)
                .Cast<IDetail>();

            var errors = transactionWithEvents.Events
                .Where(o => o.Item3 != null)
                .Select(o => o.Item3)
                .Cast<Exception>().ToList();

            if (errors.Count > 0)
            {
                Console.WriteLine(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                Console.WriteLine($"Transaction {transaction.Hash} has {errors.Count} event parsing errors:");
                Console.WriteLine("============================================");

                errors.ForEach(ex => LogException(ex, 1));

                Console.WriteLine("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

                return;
            }

            var blockNumber = transactionWithEvents.TransactionWithReceipts.Block.Number;
            var block = transactionWithEvents.TransactionWithReceipts.Block;

            if (blockNumber > lastBlockNo)
            {
                AddRequestedBlock(pgsqlConnection, block);
                lastBlockNo = blockNumber;
            }

            var classification = Classify(transactionWithEvents);
            var transactionWithExtractedDetails = (
                block.Transactions.Length
                , transaction.Hash!.ToString()
                , block.Timestamp.ToHexBigInteger()
                , new Nethereum.RPC.Eth.DTOs.Transaction
                {
                    From = transaction.SenderAddress.ToString(false),
                    To = transaction.To.ToString(false),
                    Value = new HexBigInteger(transaction.Value.ToHexString(false)),
                    BlockHash = block.Hash.ToString(),
                    TransactionHash = transaction.Hash.ToString(),
                    TransactionIndex = transaction.PoolIndex.ToHexBigInteger()
                }
                , default(TransactionReceipt?)
                , classification
                , events.ToArray());

            await TransactionsWriter.WriteTransactions(
                ConvertDatabaseUrlToConnectionString(connectionString, sslMode, trustServerCertificate),
                new List<(
                    int TotalTransactionsInBlock,
                    string TxHash,
                    HexBigInteger Timestamp,
                    Nethereum.RPC.Eth.DTOs.Transaction Transaction,
                    TransactionReceipt? Receipt,
                    TransactionClass Classification,
                    IDetail[] Details)>
                {
                    transactionWithExtractedDetails
                });
        });

        // Retry the following 3 times
        var triesLeft = 3;
        while (triesLeft-- > 0)
        {
            try
            {
                Console.WriteLine("Importing from staging...");
                ImportProcedure.ImportFromStaging(pgsqlConnection, 10);
                break;
            }
            catch (Exception ex)
            {
                if (triesLeft > 0)
                {
                    Console.WriteLine($"Error: {ex.Message}. Retrying...");
                    LogException(ex);

                    await Task.Delay(1000);
                }
                else
                {
                    throw;
                }
            }
        }

        await pgsqlConnection.CloseAsync();
    }

    private static async Task Materialize(IEnumerable<TransactionWithEvents> transactionsWithEvents,
        Func<TransactionWithEvents, Task> action)
    {
        foreach (var transactionsWithEvent in transactionsWithEvents)
        {
            await action(transactionsWithEvent);
        }
    }

    private static void AddRequestedBlock(NpgsqlConnection pgsqlConnection, Block block)
    {
        BlockTracker.AddRequested(pgsqlConnection, block.Number);

        if (block.Transactions.Length != 0)
        {
            return;
        }

        BlockTracker.InsertEmptyBlock(pgsqlConnection, (long)block.Timestamp, block.Number, block.Hash.ToString());
    }

    private static void LogException(Exception exception, int initialLevel = 0)
    {
        var level = initialLevel;
        var currentException = exception;

        while (currentException != null)
        {
            Console.WriteLine($"{new string(' ', level * 2)}{exception.Message}");
            Console.WriteLine($"{new string(' ', level * 2)}{exception.StackTrace}");

            currentException = currentException.InnerException;
            level++;
        }
    }

    private static string ConvertDatabaseUrlToConnectionString(string databaseUrl, SslMode sslMode,
        bool trustServerCertificate)
    {
        var databaseUri = new Uri(databaseUrl);
        var userInfo = databaseUri.UserInfo.Split(':');

        return new NpgsqlConnectionStringBuilder
        {
            Host = databaseUri.Host,
            Port = databaseUri.Port,
            Database = databaseUri.LocalPath.TrimStart('/'),
            Username = userInfo[0],
            Password = userInfo[1],
            SslMode = sslMode,
            TrustServerCertificate = trustServerCertificate
        }.ToString();
    }

    private static TransactionClass Classify(TransactionWithEvents transactionWithEvents)
    {
        var classification = TransactionClass.Unknown;
        foreach (var loggedEvent in transactionWithEvents.Events)
        {
            switch (loggedEvent.Item1)
            {
                case TransactionClass.Erc20Transfer:
                    classification |= TransactionClass.Erc20Transfer;
                    break;
                case TransactionClass.CrcSignup:
                    classification |= TransactionClass.CrcSignup;
                    break;
                case TransactionClass.CrcTrust:
                    classification |= TransactionClass.CrcTrust;
                    break;
                case TransactionClass.CrcOrganisationSignup:
                    classification |= TransactionClass.CrcOrganisationSignup;
                    break;
                case TransactionClass.CrcHubTransfer:
                    classification |= TransactionClass.CrcHubTransfer;
                    break;
                case TransactionClass.SafeEthTransfer:
                    classification |= TransactionClass.SafeEthTransfer;
                    break;
                case TransactionClass.EoaEthTransfer:
                    classification |= TransactionClass.EoaEthTransfer;
                    break;
            }
        }

        return classification;
    }

    #region Default implementation

    public async Task InitNetworkProtocol()
    {
    }

    public async Task InitRpcModules()
    {
    }

    public async ValueTask DisposeAsync()
    {
    }

    #endregion
}
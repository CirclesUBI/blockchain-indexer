using CirclesLand.BlockchainIndexer.Persistence;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethermind.Api;
using Nethermind.Api.Extensions;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Crypto;
using Nethermind.Logging;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.NethermindPlugin;

public record TransactionWithReceipt(Block Block, Transaction Transaction, TxReceipt? Receipt);

public record TransactionWithEvents(TransactionWithReceipt TransactionWithReceipts, 
    IList<(TransactionClass EventClassification, IDetail? Detail, Exception? Exception)> Events, TransactionClass TransactionClassification);

public class Plugin : INethermindPlugin
{
    public string Name => "CirclesLand.BlockchainIndexer.NethermindPlugin";
    public string Description => "CirclesLand.BlockchainIndexer.NethermindPlugin";
    public string Author => "Gnosis Service GmbH";

    public async Task Init(INethermindApi nethermindApi)
    {
        long from = 0;
        AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);

        Task.Run(async () =>
        {
            EthereumEcdsa ecdsa = new(nethermindApi.ChainSpec.ChainId, LimboLogs.Instance);
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

                await Insert(connectionString, sslMode, true, transactionsWithEvents, ecdsa);

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

            TransactionClass txClass = TransactionClass.Unknown;
            foreach (var (eventClassification, _, _) in events)
            {
                txClass |= eventClassification;
            }

            yield return new TransactionWithEvents(transactionWithReceipt, events, txClass);
        }
    }

    private async Task Insert(string connectionString, SslMode sslMode, bool trustServerCertificate,
        IEnumerable<TransactionWithEvents> transactionsWithEvents,
        EthereumEcdsa ecdsa)
    {
        var pgsqlConnection =
            new NpgsqlConnection(ConvertDatabaseUrlToConnectionString(connectionString, SslMode.Prefer, true));
        await pgsqlConnection.OpenAsync();

        long lastBlockNo = -1;

        await Materialize(transactionsWithEvents, async transactionWithEvents =>
        {
            var transaction = transactionWithEvents.TransactionWithReceipts.Transaction;
            Console.WriteLine($"Indexing transaction {transaction.Hash}...");

            // var receipt = transactionWithEvents.TransactionWithReceipts.Receipt;
            var events = transactionWithEvents.Events
                .Where(o => o.Item2 != null)
                .Select(o => o.Item2)
                .Cast<IDetail>()
                .ToArray();

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
                Console.WriteLine($"AddRequestedBlock({blockNumber})");
                AddRequestedBlock(pgsqlConnection, block, events);
                lastBlockNo = blockNumber;
            }

            if (transaction.To == null)
            {
                Console.WriteLine($"Encountered a contract creation transaction {transaction.Hash}. Skipping ..");
                return;
            }

            await TransactionsWriter.WriteTransactions(
                ConvertDatabaseUrlToConnectionString(connectionString, sslMode, trustServerCertificate), new [] {transactionWithEvents});

            // Retry the following 3 times
            var triesLeft = 3;
            while (triesLeft-- > 0)
            {
                try
                {
                    Console.WriteLine("Importing from staging...");
                    ImportProcedure.ImportFromStaging(pgsqlConnection, 10);
                    Persistence.StagingTables.CleanImported(pgsqlConnection);
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
        });

        await pgsqlConnection.CloseAsync();
    }

    private static async Task Materialize(IEnumerable<TransactionWithEvents> transactionsWithEvents,
        Func<TransactionWithEvents, Task> action)
    {
        foreach (var transactionsWithEvent in transactionsWithEvents)
        {
            try
            {
                await action(transactionsWithEvent);
            }
            catch (Exception e)
            {
                LogException(e);
            }
        }
    }

    private static void AddRequestedBlock(NpgsqlConnection pgsqlConnection, Block block, IDetail[] events)
    {
        BlockTracker.AddRequested(pgsqlConnection, block.Number);

        if (events.Length != 0)
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
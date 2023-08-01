using CirclesLand.BlockchainIndexer.DetailExtractors;
using CirclesLand.BlockchainIndexer.Persistence;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;
using Nethereum.Web3;
using Newtonsoft.Json.Linq;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.FixDb;

record Gap(long Start, long End, long Size);

record TransactionWithReceipt(BlockWithTransactions Block, Transaction Transaction, TransactionReceipt Receipt);

record TransactionWithEvents(TransactionWithReceipt TransactionWithReceipts, IList<(TransactionClass, IDetail?, Exception?)> Events);

static class Program
{
    const string GapQuery = @"-- Are all blocks continuous?
WITH blocks AS (
    SELECT number,
           LAG(number) OVER (ORDER BY number) AS prev_number
    FROM public.block
), cte2 as (
    SELECT prev_number + 1 AS gap_start
         , number - 1      AS gap_end
    FROM blocks
    WHERE number - prev_number > 1)
select gap_start, gap_end, gap_end - gap_start + 1 as size
from cte2
where gap_start > 12529457
order by gap_start;";

    static async Task Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine(
                "Usage: CirclesLand.BlockchainIndexer.FixDb.exe -c <connection string> -r <rpc url> [-f <true|false>]");
            return;
        }
        
        // This is O.K. because all dates are UTC
        AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);

        var parsedArgs = ParseArgs(args);
        var connectionString = (string)parsedArgs["c"];
        var rpcUrl = (string)parsedArgs["r"];
        var fix = parsedArgs.TryGetValue("f", out var f) && f is "true";
        var sslMode = parsedArgs.TryGetValue("s", out var s) && s is "true" ? SslMode.Require : SslMode.Prefer;

        var gaps = CheckForGaps(connectionString, sslMode, true);
        var blocks = DownloadBlocks(rpcUrl, gaps);
        var transactionsWithReceipt = DownloadReceipts(blocks, rpcUrl);
        var transactionsWithEvents = ExtractEvents(transactionsWithReceipt);
        
        if (!fix)
        {
            using var enumerator = transactionsWithEvents.GetEnumerator();
            while (enumerator.MoveNext()) {}
            return;
        }
        
        await InsertMissingData(connectionString, sslMode, true, transactionsWithEvents);
    }

    private static async Task InsertMissingData(string connectionString, SslMode sslMode, bool trustServerCertificate, IEnumerable<TransactionWithEvents> transactionsWithEvents)
    {
        var pgsqlConnection = new NpgsqlConnection(ConvertDatabaseUrlToConnectionString(connectionString, SslMode.Prefer, true));
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
                Console.WriteLine($"Transaction {transaction.TransactionHash} has {errors.Count} event parsing errors:");
                Console.WriteLine("============================================");
                
                errors.ForEach(ex => LogException(ex, 1));
                
                Console.WriteLine("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                
                return;
            }
            
            var blockNumber = transaction.BlockNumber.ToLong();
            var block = transactionWithEvents.TransactionWithReceipts.Block;
            var blockTimestamp = block.Timestamp.ToLong();
            var blockTimestampDateTime =
                DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;
            
            if (blockNumber > lastBlockNo)
            {
                InsertBlock(pgsqlConnection, block, blockTimestampDateTime, transaction);
                lastBlockNo = blockNumber;
            }
            
            var transactionWithExtractedDetails = (
                block.Transactions.Length
                , transaction.TransactionHash
                , block.Timestamp
                , transaction
                , receipt
                , TransactionClass.Unknown
                , events.ToArray());

            await TransactionsWriter.WriteTransactions(ConvertDatabaseUrlToConnectionString(connectionString, sslMode, trustServerCertificate), 
                new List<(
                    int TotalTransactionsInBlock,
                    string TxHash,
                    HexBigInteger Timestamp,
                    Transaction Transaction,
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

    private static void InsertBlock(NpgsqlConnection pgsqlConnection, BlockWithTransactions block,
        DateTime blockTimestampDateTime, Transaction transaction)
    {
        BlockTracker.AddRequested(pgsqlConnection, block.Number.ToLong());

        if (block.Transactions.Length == 0)
        {
            BlockTracker.InsertEmptyBlock(pgsqlConnection, block);
        }
        else
        {
            BlockWriter.WriteBlocks(pgsqlConnection, TransactionsWriter.blockTableName,
                new List<(long BlockNumber, DateTime BlockTimestamp, string Hash, int TotalTransactionCount)>
                {
                    (block.Number.ToLong(), blockTimestampDateTime, transaction.BlockHash, block.Transactions.Length)
                });
        }
    }

    private static async Task Materialize(IEnumerable<TransactionWithEvents> transactionsWithEvents, Func<TransactionWithEvents, Task> action)
    {
        foreach (var transactionsWithEvent in transactionsWithEvents)
        {
            await action(transactionsWithEvent);
        }
    }

    private static IEnumerable<TransactionWithEvents> ExtractEvents(IEnumerable<TransactionWithReceipt> transactionsWithReceipt)
    {
        foreach (var transactionWithReceipt in transactionsWithReceipt)
        {
            var transaction = transactionWithReceipt.Transaction;
            var receipt = transactionWithReceipt.Receipt;
            var events = new List<(TransactionClass, IDetail?, Exception?)>();

            foreach (var log in receipt.Logs)
            {
                ExtractCrcTrust(log, events);
                ExtractCrcSignup(receipt, events);
                ExtractCrcHubTransfer(receipt, events);
                ExtractCrcOrganisationSignup(log, events);
                ExtractErc20Transfer(log, events);
            }

            ExtractEoaEthTransfer(transaction, receipt, events);
            ExtractSafeEthTransfer(transaction, receipt, events);
            
            yield return new TransactionWithEvents(transactionWithReceipt, events);
        }
    }

    private static void ExtractCrcTrust(JToken log, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (!TransactionClassifier.IsCrcTrust(log, out var canSendTo, out var user, out var limit))
            {
                return;
            }
            
            Console.WriteLine($"* CRC Trust: {user} -> {canSendTo} ({limit})");
            events.Add((TransactionClass.CrcTrust, new CrcTrust
            {
                Address = user,
                CanSendTo = canSendTo,
                Limit = limit.ToLong()
            }, null));
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    private static void ExtractCrcSignup(TransactionReceipt receipt, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (!TransactionClassifier.IsCrcSignup(receipt, out var userAddress, out var tokenAddress))
            {
                return;
            }
            
            Console.WriteLine($"* CRC Signup: {userAddress} -> {tokenAddress}");
            events.Add((TransactionClass.CrcSignup, new CrcSignup
            {
                User = userAddress,
                Token = tokenAddress
            }, null));
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    private static void ExtractCrcHubTransfer(TransactionReceipt receipt, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (!TransactionClassifier.IsCrcHubTransfer(receipt, out var from, out var to, out var amount))
            {
                return;
            }
            
            Console.WriteLine($"* CRC Hub Transfer: {from} -> {to} ({amount})");
            events.Add((TransactionClass.CrcHubTransfer, new CrcHubTransfer
            {
                From = from,
                To = to,
                Value = amount?.ToString()
            }, null));
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    private static void ExtractCrcOrganisationSignup(JToken log, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (!TransactionClassifier.IsCrcOrganisationSignup(log, out var organisationAddress))
            {
                return;
            }
            
            Console.WriteLine($"* CRC Organisation Signup: {organisationAddress}");
            events.Add((TransactionClass.CrcOrganisationSignup, new CrcOrganisationSignup
            {
                Organization = organisationAddress
            }, null));
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    private static void ExtractErc20Transfer(JToken log, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (!TransactionClassifier.IsErc20Transfer(log, out var tokenAddress, out var from, out var to, out var erc20Amount))
            {
                return;
            }
            
            Console.WriteLine($"* ERC20 Transfer: {from} -> {to} ({erc20Amount})");
            events.Add((TransactionClass.Erc20Transfer, new Erc20Transfer
            {
                Token = tokenAddress,
                From = from,
                To = to,
                Value = erc20Amount?.ToString()
            }, null));
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    private static void ExtractEoaEthTransfer(Transaction transaction, TransactionReceipt receipt, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (TransactionClassifier.IsEoaEthTransfer(transaction, receipt, out var from, out var to,
                    out var ethAmount))
            {
                Console.WriteLine($"* EOA ETH Transfer: {from} -> {to} ({ethAmount})");
                events.Add((TransactionClass.EoaEthTransfer, new EthTransfer
                {
                    From = from,
                    To = to,
                    Value = ethAmount?.ToString()
                }, null));
            }
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    private static void ExtractSafeEthTransfer(Transaction transaction, TransactionReceipt receipt, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (TransactionClassifier.IsSafeEthTransfer(transaction, receipt, out var initiator, out var from,
                    out var to, out var ethAmount))
            {
                Console.WriteLine($"* Safe ETH Transfer: {from} -> {to} ({ethAmount}) by {initiator}");
                events.Add((TransactionClass.SafeEthTransfer, new GnosisSafeEthTransfer
                {
                    Initiator = initiator,
                    From = from,
                    To = to,
                    Value = ethAmount?.ToString()
                }, null));
            }
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    private static IEnumerable<TransactionWithReceipt> DownloadReceipts(IEnumerable<BlockWithTransactions> blocks, string rpcUrl)
    {
        foreach (var block in blocks)
        {
            var receipts = DownloadReceipts(rpcUrl, block)
                .ToDictionary(o => o.TransactionHash);

            foreach (var transaction in block.Transactions)
            {
                yield return new TransactionWithReceipt(block, transaction, receipts[transaction.TransactionHash]);
            }
        }
    }

    private static IEnumerable<TransactionReceipt> DownloadReceipts(string rpcUrl, BlockWithTransactions block)
    {
        Console.WriteLine($"Downloading receipts");
        
        var web3 = new Web3(rpcUrl);
        foreach (var transaction in block.Transactions)
        {
            Console.WriteLine($"* Downloading receipt for transaction {transaction.TransactionHash}...");
            var receipt = web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(transaction.TransactionHash).Result;
            
            yield return receipt;
        }

        Console.WriteLine();
    }

    private static IEnumerable<BlockWithTransactions> DownloadBlocks(string rpcUrl, IEnumerable<Gap> gaps)
    {
        Console.WriteLine($"Downloading blocks");
        
        var web3 = new Web3(rpcUrl);
        foreach (var gap in gaps)
        {
            var start = gap.Start;
            var end = gap.End;
            var size = gap.Size;

            Console.WriteLine($"* Downloading {size} blocks from {start} to {end}...");
            for (var i = start; i <= end; i++)
            {
                Console.WriteLine($"  - Downloading block {i}...");
                var blocks = web3.Eth.Blocks.GetBlockWithTransactionsByNumber
                    .SendRequestAsync(new HexBigInteger(start))
                    .Result;
                yield return blocks;
            }
        }

        Console.WriteLine();
    }

    private static string ConvertDatabaseUrlToConnectionString(string databaseUrl, SslMode sslMode, bool trustServerCertificate)
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

    private static IEnumerable<Gap> CheckForGaps(string connectionString, SslMode sslMode, bool trustServerCertificate)
    {
        Console.WriteLine($"Checking for gaps");
        using var pgsqlConnection = new NpgsqlConnection(ConvertDatabaseUrlToConnectionString(connectionString, sslMode, trustServerCertificate));
        pgsqlConnection.Open();

        using var cmd = new NpgsqlCommand(GapQuery, pgsqlConnection);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var start = reader.GetInt64(0);
            var end = reader.GetInt64(1);
            var size = reader.GetInt64(2);
            Console.WriteLine($"* {start} - {end} ({size})");
            yield return new Gap(start, end, size);
        }

        Console.WriteLine();
    }

    /// <summary>
    /// Parse the following cmd args:
    /// -c connection string (string)
    /// -r rpc url (string)
    /// -f fix (true|false)
    /// -s database ssl (true|false)
    /// </summary>
    static Dictionary<string, object> ParseArgs(string[] args)
    {
        var parsedArgs = new Dictionary<string, object>();
        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg.StartsWith('-'))
            {
                var key = arg.Substring(1);
                var value = args[i + 1];
                parsedArgs.Add(key, value);
            }
        }

        if (!parsedArgs.ContainsKey("c"))
        {
            throw new Exception("Missing connection string argument (-c)");
        }

        if (!parsedArgs.ContainsKey("r"))
        {
            throw new Exception("Missing rpc url argument (-r)");
        }

        if (parsedArgs.TryGetValue("f", out var fix) && fix is not "true" and not "false")
        {
            throw new Exception("Invalid fix argument (-f). Allowed values: true, false");
        }

        if (parsedArgs.TryGetValue("s", out var ssl) && ssl is not "true" and not "false")
        {
            throw new Exception("Invalid ssl argument (-s). Allowed values: true, false");
        }

        return parsedArgs;
    }
}
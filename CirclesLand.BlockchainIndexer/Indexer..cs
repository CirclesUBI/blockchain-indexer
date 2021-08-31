using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using CirclesLand.BlockchainIndexer.DetailExtractors;
using CirclesLand.BlockchainIndexer.Persistence;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using CirclesLand.BlockchainIndexer.Util;
using Dapper;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.Web3;
using Npgsql;

namespace CirclesLand.BlockchainIndexer
{
    public class Indexer
    {
        private static readonly RestartSettings _restartSettings = RestartSettings.Create(
            minBackoff: TimeSpan.FromSeconds(3),
            maxBackoff: TimeSpan.FromSeconds(30),
            randomFactor: 0.2 // adds 20% "noise" to vary the intervals slightly
        ).WithMaxRestarts(20, TimeSpan.FromMinutes(5)); // limits the amount of restarts to 20 within 5 minutes

        private readonly Web3 _web3;
        private readonly string _connectionString;

        public Indexer(
            string connectionString,
            string rpcEndpointUrl)
        {
            _connectionString = connectionString;
            _web3 = new Web3(rpcEndpointUrl);
        }

        private long? _lastBlock = null;

        public Source<HexBigInteger, NotUsed> CreateSource()
        {
            return RestartSource.WithBackoff(() =>
                Source.UnfoldAsync(new HexBigInteger(0), async lastBlock =>
                {
                    try
                    {
                        if (_lastBlock == null)
                        {
                            await using var connection = new NpgsqlConnection(_connectionString);
                            connection.Open();
                            
                            var lastIndexedBlock =
                                connection.QuerySingleOrDefault<long?>("select max(number) from block") ?? 12529458;

                            _lastBlock = lastIndexedBlock;
                        }

                        var nextBlock = _lastBlock + 1;
                        _lastBlock = nextBlock;
                        
                        return new Option<(HexBigInteger, HexBigInteger)>((
                            new HexBigInteger(nextBlock.Value),
                            new HexBigInteger(nextBlock.Value)));
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex.Message);
                        if (ex.StackTrace != null) Logger.LogError(ex.StackTrace);

                        throw;
                    }
                }), _restartSettings);
        }


        public void Start()
        {
            long downloadedBlocks = 0;
            long downloadedTransactionReceipts = 0;
            long writtenTransactions = 0;
            
            DateTime startedAt = DateTime.Now;
        
            var system = ActorSystem.Create("system");
            var materializer = system.Materializer();

            // Check for new blocks every 500ms and emit the block no. every time it changed
            CreateSource()

                // Buffer up to 25 block nos. in case that the downstream processing is not fast enough.
                // When the buffer size is exceeded the whole stream will fail.
                // .Buffer(25, OverflowStrategy.Fail) // TODO: This doesn't suite to "catch up" mode

                // Get the full block with all transactions
                .SelectAsync(24, currentBlockNo =>
                {
                    try
                    {
                        return _web3.Eth.Blocks
                            .GetBlockWithTransactionsByNumber
                            .SendRequestAsync(currentBlockNo);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex.Message);
                        if (ex.StackTrace != null) Logger.LogError(ex.StackTrace);

                        throw;
                    }
                })

                // Bundle the every transaction in a block with the block timestamp and send it downstream
                .SelectMany(block =>
                {
                    try
                    {
                        Interlocked.Increment(ref downloadedBlocks);
                        // Console.WriteLine($"  Found {block.Transactions.Length} transactions in block {block.Number}");

                        return block.Transactions
                            .Select(o => (
                                TotalTransactionsInBlock: block.Transactions.Length,
                                Timestamp: block.Timestamp,
                                Transaction: o));
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex.Message);
                        if (ex.StackTrace != null) Logger.LogError(ex.StackTrace);

                        throw;
                    }
                })

                .Buffer(1024, OverflowStrategy.Backpressure)
                
                // Add the receipts for every transaction
                .SelectAsync(96, async timestampAndTransaction =>
                {
                    try
                    {
                        var receipt = await _web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(
                            timestampAndTransaction.Transaction.TransactionHash);

                        Interlocked.Increment(ref downloadedTransactionReceipts);
                        
                        return (
                            TotalTransactionsInBlock: timestampAndTransaction.TotalTransactionsInBlock,
                            Timestamp: timestampAndTransaction.Timestamp,
                            Transaction: timestampAndTransaction.Transaction,
                            Receipt: receipt
                        );
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex.Message);
                        if (ex.StackTrace != null) Logger.LogError(ex.StackTrace);

                        throw;
                    }
                })

                .Buffer(4096, OverflowStrategy.Backpressure)
                
                // Classify all transactions
                .Select(transactionAndReceipt =>
                {
                    try
                    {
                        var classification = TransactionClassifier.Classify(
                            transactionAndReceipt.Transaction,
                            transactionAndReceipt.Receipt,
                            null);

                        return (
                            TotalTransactionsInBlock: transactionAndReceipt.TotalTransactionsInBlock,
                            Timestamp: transactionAndReceipt.Timestamp,
                            Transaction: transactionAndReceipt.Transaction,
                            Receipt: transactionAndReceipt.Receipt,
                            Classification: classification
                        );
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex.Message);
                        if (ex.StackTrace != null) Logger.LogError(ex.StackTrace);

                        throw;
                    }
                })

                // Set a flag which indicates whether to store the transaction or not
                .Select(transactionAndReceipt =>
                {
                    try
                    {
                        var isUnknown = transactionAndReceipt.Classification == TransactionClass.Unknown;
                        /*
                        if (isUnknown)
                        {
                            Console.WriteLine(
                                $"    Tx {transactionAndReceipt.Transaction.TransactionIndex}: Not classified.");
                        }
                        else
                        {
                            Console.WriteLine(
                                $"    Tx {transactionAndReceipt.Transaction.TransactionIndex}: {transactionAndReceipt.Classification}");
                        }
                        */

                        return (
                            TotalTransactionsInBlock: transactionAndReceipt.TotalTransactionsInBlock,
                            Timestamp: transactionAndReceipt.Timestamp,
                            Transaction: transactionAndReceipt.Transaction,
                            Receipt: transactionAndReceipt.Receipt,
                            Classification: transactionAndReceipt.Classification,
                            ShouldBeIndexed: !isUnknown
                        );
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex.Message);
                        if (ex.StackTrace != null) Logger.LogError(ex.StackTrace);

                        throw;
                    }
                })

                // Add the details for each transaction
                .Select(classifiedTransactions =>
                {
                    try
                    {
                        var extractedDetails = TransactionDetailExtractor.Extract(
                                classifiedTransactions.Classification,
                                classifiedTransactions.Transaction,
                                classifiedTransactions.Receipt)
                            .ToArray();

                        return (
                            TotalTransactionsInBlock: classifiedTransactions.TotalTransactionsInBlock,
                            TxHash: classifiedTransactions.Transaction.TransactionHash,
                            Timestamp: classifiedTransactions.Timestamp,
                            Transaction: classifiedTransactions.Transaction,
                            Receipt: classifiedTransactions.Receipt,
                            Classification: classifiedTransactions.Classification,
                            SholdBeIndexed: classifiedTransactions.ShouldBeIndexed,
                            Details: extractedDetails
                        );
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex.Message);
                        if (ex.StackTrace != null) Logger.LogError(ex.StackTrace);

                        throw;
                    }
                })

                .Buffer(8192, OverflowStrategy.Backpressure)
                
                .RunForeach(transactionWithExtractedDetails =>
                {
                  /*  Console.WriteLine(
                        $"      Writing '{transactionWithExtractedDetails.Transaction.TransactionHash}' to the db");
*/
                    using var connection = new NpgsqlConnection(_connectionString);
                    connection.Open();

                    // "Read committed"-isolation level should be sufficient because the data will not
                    // be updated again once its written.
                    using var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

                    try
                    {
                        var blockTimestamp = ((HexBigInteger) transactionWithExtractedDetails.Timestamp).ToLong();
                        var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(
                            blockTimestamp).UtcDateTime;

                        var transactionId = new TransactionWriter(connection, transaction).Write(
                            !transactionWithExtractedDetails.SholdBeIndexed,
                            transactionWithExtractedDetails.TotalTransactionsInBlock,
                            blockTimestampDateTime,
                            transactionWithExtractedDetails.Classification,
                            transactionWithExtractedDetails.Transaction,
                            transactionWithExtractedDetails.Details);

                        if (transactionId != null)
                        {
                            var detailIds = new TransactionDetailWriter(connection, transaction).Write(
                                transactionId.Value,
                                transactionWithExtractedDetails.Details);
                        }

                        transaction.Commit();
                        Interlocked.Increment(ref writtenTransactions);

                        if (writtenTransactions % 1000 == 0)
                        {
                            var defaultColor = Console.ForegroundColor;
                            Console.ForegroundColor = ConsoleColor.Cyan;
                            Console.WriteLine($"----");
                            Console.WriteLine($"Downloaded {downloadedBlocks} blocks since {startedAt}");
                            Console.WriteLine($"Downloaded {downloadedTransactionReceipts} receipts since {startedAt}");
                            Console.WriteLine($"Wrote {writtenTransactions} transactions since {startedAt}");
                            var elapsedTime = DateTime.Now - startedAt;
                            Console.WriteLine($"Performance: {downloadedBlocks / elapsedTime.TotalSeconds} downloaded blocks per second");
                            Console.WriteLine($"Performance: {downloadedTransactionReceipts / elapsedTime.TotalSeconds} downloaded receipts per second");
                            Console.WriteLine($"Performance: {writtenTransactions / elapsedTime.TotalSeconds} written transactions per second");
                            Console.ForegroundColor = defaultColor;
                        }
/*
                        Console.WriteLine(
                            $"      Successfully wrote '{transactionWithExtractedDetails.Transaction.TransactionHash}' to the db");
                            */
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();

                        Logger.LogError(
                            $"      Failed to write '{transactionWithExtractedDetails.Transaction.TransactionHash}' to the db");
                        Logger.LogError(ex.Message);
                        if (ex.StackTrace != null) Logger.LogError(ex.StackTrace);
                    }
                }, materializer);
        }
    }
}
using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Implementation.Fusing;
using CirclesLand.BlockchainIndexer.DetailExtractors;
using CirclesLand.BlockchainIndexer.Persistence;
using CirclesLand.BlockchainIndexer.Sources;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using CirclesLand.BlockchainIndexer.Util;
using Dapper;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.Web3;
using Npgsql;

namespace CirclesLand.BlockchainIndexer
{
    public class IndexedBlockEventArgs : EventArgs
    {
        public HexBigInteger Block { get; }

        public IndexedBlockEventArgs(HexBigInteger block)
        {
            Block = block;
        }
    }

    public enum IndexerMode
    {
        NotRunning,
        CatchUp,
        Polling,
        Live
    }

    public class Indexer
    {
        private readonly string _rpcEndpointUrl;
        private readonly string _connectionString;

        public event EventHandler<IndexedBlockEventArgs> NewBlock;

        public Indexer(
            string connectionString,
            string rpcEndpointUrl)
        {
            _connectionString = connectionString;
            _rpcEndpointUrl = rpcEndpointUrl;
        }

        private long? _firstNewBlock;

        public IndexerMode Mode { get; private set; } = IndexerMode.NotRunning;

        public async Task Run(int maxBlockDownloads = 24, int maxReceiptDownloads = 96)
        {
            HexBigInteger currentlyWritingBlock = new(0);
            var clientId = Guid.NewGuid().ToString("N");

            var startedAt = DateTime.Now;
            var system = ActorSystem.Create("system");
            var materializer = system.Materializer();

            // Total Stats:
            long totalBlocksDownloaded = 0;
            long totalProcessedTransactions = 0;
            long totalProcessedBlocks = 0;
            long totalDownloadedTransactions = 0;
            long totalErrors = 0;
            long totalRounds = 0;

            var errorRestartPenaltyInMs = 0;
            const int maxErrorRestartPenalty = 3 * 60 * 1000;
            var currentErrorRestarts = 0;
            Exception? lastRoundError = null;

            void ResetErrorPenalty()
            {
                lastRoundError = null;
                currentErrorRestarts = 0;
                errorRestartPenaltyInMs = 0;
            }

            // await LiveSource.NewBlockHeader_With_Subscription(_rpcEndpointUrl);

            while (true)
            {
                Interlocked.Increment(ref totalRounds);
                Logger.Log($"Starting round {totalRounds} ..");

                try
                {
                    if (lastRoundError != null && errorRestartPenaltyInMs > 0)
                    {
                        Logger.Log($"Waiting for {TimeSpan.FromMilliseconds(errorRestartPenaltyInMs)} before " +
                                   $"starting again after an error.");

                        await Task.Delay(errorRestartPenaltyInMs);
                    }

                    await using var writerConnection = new NpgsqlConnection(_connectionString);
                    Logger.Log($"Opening the writer db connection ..");
                    writerConnection.Open();
                    
                    Source<HexBigInteger, NotUsed> source;
                    var web3 = new Web3(_rpcEndpointUrl);

                    Logger.Log("Getting the currently latest block number ..");
                    var currentBlock = await web3.Eth.Blocks
                        .GetBlockNumber.SendRequestAsync();

                    Logger.Log($"The current block is {currentBlock}.");

                    var lastKnownBlock = writerConnection.QuerySingleOrDefault<long?>(
                        "select max(number) from block where total_Transaction_count = indexed_transaction_count;") 
                                 ?? 12529458;

                    _firstNewBlock = lastKnownBlock + 1;
                    
                    Logger.Log($"First new known block is: {_firstNewBlock}");
                    
                    var delta = currentBlock.Value - _firstNewBlock;
                    Logger.Log($"The last known block is {delta} blocks away from the first new block.");

                    if (delta > 10)
                    {
                        Logger.Log($"delta > 10: Using the bulk source.");
                        source = BulkSource.Create(_firstNewBlock.Value, currentBlock.Value);
                        Mode = IndexerMode.CatchUp;
                    }
                    else
                    {
                        Logger.Log($"delta <= 10: Using the polling source.");
                        source = IntervalSource.Create(500, _connectionString, _rpcEndpointUrl);
                        Mode = IndexerMode.Polling;
                    }

                    await source
                        // Get the full block with all transactions
                        .SelectAsync(maxBlockDownloads, currentBlockNo =>
                            web3.Eth.Blocks
                                .GetBlockWithTransactionsByNumber
                                .SendRequestAsync(currentBlockNo))

                        // Bundle the every transaction in a block with the block timestamp and send it downstream
                        .SelectMany(block =>
                        {
                            Interlocked.Increment(ref totalBlocksDownloaded);

                            var transactions = block.Transactions
                                .Select(o => (
                                    TotalTransactionsInBlock: block.Transactions.Length,
                                    Timestamp: block.Timestamp,
                                    Transaction: o))
                                .ToArray();

                            return transactions;
                        })
                        .Buffer(maxBlockDownloads * 4, OverflowStrategy.Backpressure)

                        // Add the receipts for every transaction
                        .SelectAsync(maxReceiptDownloads, async timestampAndTransaction =>
                        {
                            var receipt = await web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(
                                timestampAndTransaction.Transaction.TransactionHash);

                            return (
                                TotalTransactionsInBlock: timestampAndTransaction.TotalTransactionsInBlock,
                                Timestamp: timestampAndTransaction.Timestamp,
                                Transaction: timestampAndTransaction.Transaction,
                                Receipt: receipt
                            );
                        })
                        .Buffer(maxReceiptDownloads * 4, OverflowStrategy.Backpressure)

                        // Classify all transactions
                        .Select(transactionAndReceipt =>
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
                        })

                        // Set a flag which indicates whether to store the transaction or not
                        .Select(transactionAndReceipt =>
                        {
                            var isUnknown = transactionAndReceipt.Classification == TransactionClass.Unknown;

                            return (
                                TotalTransactionsInBlock: transactionAndReceipt.TotalTransactionsInBlock,
                                Timestamp: transactionAndReceipt.Timestamp,
                                Transaction: transactionAndReceipt.Transaction,
                                Receipt: transactionAndReceipt.Receipt,
                                Classification: transactionAndReceipt.Classification,
                                ShouldBeIndexed: !isUnknown
                            );
                        })

                        // Add the details for each transaction
                        .Select(classifiedTransactions =>
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
                        })
                        .RunForeach(transactionWithExtractedDetails =>
                        {
                            // "Read committed"-isolation level should be sufficient because the data will not
                            // be updated again once its written.
                            using var transaction = writerConnection.BeginTransaction(IsolationLevel.ReadCommitted);

                            var blockTimestamp =
                                transactionWithExtractedDetails.Timestamp.ToLong();
                            var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(
                                blockTimestamp).UtcDateTime;

                            var transactionId = new TransactionWriter(writerConnection, transaction).Write(
                                !transactionWithExtractedDetails.SholdBeIndexed,
                                transactionWithExtractedDetails.TotalTransactionsInBlock,
                                blockTimestampDateTime,
                                transactionWithExtractedDetails.Classification,
                                transactionWithExtractedDetails.Transaction,
                                transactionWithExtractedDetails.Details);

                            if (transactionId != null)
                            {
                                var detailIds = new TransactionDetailWriter(writerConnection, transaction).Write(
                                    transactionId.Value,
                                    transactionWithExtractedDetails.Details);
                            }

                            transaction.Commit();

                            if (transactionWithExtractedDetails.Transaction.BlockNumber.Value >
                                currentlyWritingBlock.Value)
                            {
                                if (Mode != IndexerMode.NotRunning && Mode != IndexerMode.CatchUp)
                                {
                                    NewBlock?.Invoke(this, new IndexedBlockEventArgs(currentlyWritingBlock));
                                }

                                currentlyWritingBlock = transactionWithExtractedDetails.Transaction.BlockNumber;
                                Interlocked.Increment(ref totalProcessedBlocks);
                            }

                            ResetErrorPenalty();
                            Interlocked.Increment(ref totalProcessedTransactions);

                            if (totalProcessedTransactions % 500 == 0)
                            {
                                var elapsedTime = DateTime.Now - startedAt;
                                var defaultColor = Console.ForegroundColor;
                                Console.ForegroundColor = ConsoleColor.Cyan;
                                Console.WriteLine(
                                    $"Downloaded {totalBlocksDownloaded} blocks @ {totalBlocksDownloaded / elapsedTime.TotalSeconds} " +
                                    $"blocks per second (avg. since {startedAt}");
                                Console.WriteLine(
                                    $"Processed {totalProcessedBlocks} blocks @ {totalProcessedBlocks / elapsedTime.TotalSeconds} " +
                                    $"blocks per second (avg. since {startedAt})");
                                Console.WriteLine(
                                    $"Downloaded {totalDownloadedTransactions} transactions @ {totalDownloadedTransactions / elapsedTime.TotalSeconds} " +
                                    $"transactions per second (avg. since {startedAt}");
                                Console.WriteLine(
                                    $"Processed {totalProcessedTransactions} transactions @ {totalProcessedTransactions / elapsedTime.TotalSeconds} " +
                                    $"transactions per second (avg. since {startedAt})");
                                Console.ForegroundColor = defaultColor;
                            }
                        }, materializer);

                    Logger.Log($"Completed the stream. Restarting ..");
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref totalErrors);
                    Interlocked.Increment(ref currentErrorRestarts);
                    lastRoundError = ex;

                    Logger.LogError(ex.Message);
                    Logger.LogError(ex.StackTrace);

                    var additionalPenalty = currentErrorRestarts * 5 * 1000 + new Random().Next(0, 2000);
                    if (errorRestartPenaltyInMs + additionalPenalty > maxErrorRestartPenalty)
                    {
                        errorRestartPenaltyInMs = maxErrorRestartPenalty;
                    }
                    else
                    {
                        errorRestartPenaltyInMs += additionalPenalty;
                    }
                }
            }
        }

        private void RemoveIncompleteBlocks(NpgsqlConnection connection)
        {
            var incompleteBlock =
                connection.QuerySingleOrDefault<long?>("select block_no from first_incomplete_block;");

            if (incompleteBlock != null)
            {
                Logger.Log($"Found block {incompleteBlock} as the earliest incomplete block. " +
                           $"Deleting all blocks from this block on ..");

                connection.Execute("call delete_incomplete_blocks();");

                Logger.Log($"Done.");
            }
            else
            {
                Logger.Log("No incomplete blocks found.");
            }
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using CirclesLand.BlockchainIndexer.ABIs;
using CirclesLand.BlockchainIndexer.DetailExtractors;
using CirclesLand.BlockchainIndexer.Persistence;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using CirclesLand.BlockchainIndexer.Util;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;

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
        public IndexerMode Mode { get; private set; } = IndexerMode.NotRunning;

        public async Task Run(CancellationToken cancellationToken)
        {
            var system = ActorSystem.Create("system");
            var materializer = system.Materializer();
            var instanceContext = new InstanceContext();

            while (!cancellationToken.IsCancellationRequested)
            {
                var roundContext = instanceContext.CreateRoundContext();
                
                try
                {
                    var roundStartsIn = roundContext.StartAt - DateTime.Now;
                    if (roundStartsIn.TotalMilliseconds > 0)
                    {
                        Logger.Log($"Round {roundContext.RoundNo} starting at {roundContext.StartAt} ..");
                        await Task.Delay(roundStartsIn, cancellationToken);
                    }

                    roundContext.Log($"Round {roundContext.RoundNo} started at {DateTime.Now}.");


                    roundContext.Log($"Finding the last persisted block ..");
                    var lastPersistedBlock = roundContext.GetLastValidBlock();
                    roundContext.Log($"Last persisted block: {lastPersistedBlock}");


                    roundContext.Log($"Finding the latest blockchain block ..");
                    var currentBlock = await roundContext
                        .Web3
                        .Eth
                        .Blocks
                        .GetBlockNumber
                        .SendRequestAsync();
                    roundContext.Log($"Latest blockchain block: {currentBlock.Value}");
                    
                    var delta = currentBlock.Value - lastPersistedBlock;
                    Source<HexBigInteger, NotUsed> source;
                    
                    int flushEveryNthRound;
                    
                    if (delta > Settings.UseBulkSourceThreshold)
                    {
                        roundContext.Log($"Found {delta} blocks to catch up. Using the 'BulkSource'.");
                        Mode = IndexerMode.CatchUp;

                        source = roundContext.SourceFactory.CreateBulkSource(
                            new HexBigInteger(lastPersistedBlock)
                            , currentBlock);
                        
                        flushEveryNthRound = Settings.BulkFlushInterval;
                    }
                    else
                    {
                        roundContext.Log($"Found {delta} blocks to catch up. Using the 'PollingSource'.");
                        Mode = IndexerMode.Polling;

                        source = roundContext.SourceFactory.CreatePollingSource();
                        flushEveryNthRound = Settings.SerialFlushInterval;
                    }

                    // roundContext.Log($"Starting with source {source?.GetType()?.Name ?? "<null>"}");
                    
                    await source
                        .Select(o =>
                        {
                            BlockTracker.AddRequested(roundContext.Connection, o.ToLong());
                            return o;
                        })
                        // Get the full block with all transactions
                        .SelectAsync(Settings.MaxParallelBlockDownloads, currentBlockNo =>
                            roundContext.Web3.Eth.Blocks
                                .GetBlockWithTransactionsByNumber
                                .SendRequestAsync(currentBlockNo))
                        .Buffer(Settings.MaxDownloadedBlockBufferSize, OverflowStrategy.Backpressure)
                        // Bundle the every transaction in a block with the block timestamp and send it downstream
                        .SelectMany(block =>
                        {
                            Interlocked.Increment(ref Statistics.TotalDownloadedBlocks);

                            var t = block.Transactions.ToArray();
                            Interlocked.Add(ref Statistics.TotalDownloadedTransactions, t.Length);

                            if (t.Length == 0)
                            {
                                BlockTracker.InsertEmptyBlock(roundContext.Connection, block);
                                CompleteBatch(flushEveryNthRound, roundContext);
                            }

                            var transactions = t
                                .Select(o => (
                                    TotalTransactionsInBlock: t.Length,
                                    Timestamp: block.Timestamp,
                                    Transaction: o))
                                .ToArray();

                            return transactions;
                        })
                        .Buffer(Settings.MaxDownloadedTransactionsBufferSize, OverflowStrategy.Backpressure)
                        // Add the receipts for every transaction
                        .SelectAsync(Settings.MaxParallelReceiptDownloads, async timestampAndTransaction =>
                        {
                            var receipt = await roundContext.Web3.Eth.Transactions.GetTransactionReceipt
                                .SendRequestAsync(
                                    timestampAndTransaction.Transaction.TransactionHash);

                            Interlocked.Increment(ref Statistics.TotalDownloadedReceipts);

                            return (
                                TotalTransactionsInBlock: timestampAndTransaction.TotalTransactionsInBlock,
                                Timestamp: timestampAndTransaction.Timestamp,
                                Transaction: timestampAndTransaction.Transaction,
                                Receipt: receipt
                            );
                        })
                        .Buffer(Settings.MaxDownloadedReceiptsBufferSize, OverflowStrategy.Backpressure)
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

                        // Add the details for each transaction
                        .SelectAsync(2, async classifiedTransactions =>
                        {
                            var extractedDetails = TransactionDetailExtractor.Extract(
                                    classifiedTransactions.Classification,
                                    classifiedTransactions.Transaction,
                                    classifiedTransactions.Receipt)
                                .ToArray();

                            // For every CrcSignup-event check who the owner is
                            var signups = extractedDetails
                                .Where(o => o is CrcSignup)
                                .Cast<CrcSignup>();
                            
                            foreach (var signup in signups)
                            {
                                var contract = roundContext.Web3.Eth.GetContract(
                                    GnosisSafeABI.Json, signup.User);
                                var function = contract.GetFunction("getOwners");
                                var owners = await function.CallAsync<List<string>>();
                                signup.Owners = owners?.Select(o => o.ToLower()).ToArray() ?? Array.Empty<string>();
                            }
                            
                            var organisationSignups = extractedDetails
                                .Where(o => o is CrcOrganisationSignup)
                                .Cast<CrcOrganisationSignup>();
                            
                            foreach (var organisationSignup in organisationSignups)
                            {
                                var contract = roundContext.Web3.Eth.GetContract(
                                    GnosisSafeABI.Json, organisationSignup.Organization);
                                var function = contract.GetFunction("getOwners");
                                var owners = await function.CallAsync<List<string>>();
                                organisationSignup.Owners = owners.Select(o => o.ToLower()).ToArray();
                            }
                            
                            return (
                                TotalTransactionsInBlock: classifiedTransactions.TotalTransactionsInBlock,
                                TxHash: classifiedTransactions.Transaction.TransactionHash,
                                Timestamp: classifiedTransactions.Timestamp,
                                Transaction: classifiedTransactions.Transaction,
                                Receipt: classifiedTransactions.Receipt,
                                Classification: classifiedTransactions.Classification,
                                Details: extractedDetails
                            );
                        })
                        .GroupedWithin(Settings.WriteToStagingBatchSize,
                            TimeSpan.FromSeconds(Settings.WriteToStagingBatchMaxIntervalInSeconds))
                        .Buffer(Settings.MaxWriteToStagingBatchBufferSize, OverflowStrategy.Backpressure)
                        .RunForeach(transactionsWithExtractedDetails =>
                        {
                            roundContext.Log($" Writing batch to staging tables ..");

                            var txArr = transactionsWithExtractedDetails.ToArray();
                            
                            TransactionsWriter.WriteTransactions(
                                roundContext.Connection,
                                txArr);

                            CompleteBatch(flushEveryNthRound, roundContext);
                        }, materializer);

                    Logger.Log($"Completed the stream. Restarting ..");
                }
                catch (Exception ex)
                {
                    roundContext.OnError(ex);
                }
            }
        }

        private void CompleteBatch(int flushEveryNthRound, RoundContext roundContext)
        {
            string[] writtenTransactions = { };
            if (Statistics.TotalProcessedBatches % flushEveryNthRound == 0)
            {
                roundContext.Log($" Importing from staging tables ..");
                ImportProcedure.ImportFromStaging(roundContext.Connection
                    , Mode == IndexerMode.CatchUp
                        ? Settings.BulkFlushTimeoutInSeconds
                        : Settings.SerialFlushTimeoutInSeconds);

                roundContext.Log($" Cleaning staging tables ..");
                writtenTransactions = StagingTables.CleanImported(roundContext.Connection);
            }

            if ((Mode == IndexerMode.Polling || Mode == IndexerMode.Live)
                && writtenTransactions.Length > 0)
            {
                roundContext.OnBatchSuccessNotify(writtenTransactions);
            }
            else
            {
                roundContext.OnBatchSuccess();
            }
        }
    }
}
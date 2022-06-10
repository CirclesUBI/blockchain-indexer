using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util.Internal;
using CirclesLand.BlockchainIndexer.ABIs;
using CirclesLand.BlockchainIndexer.Api;
using CirclesLand.BlockchainIndexer.DetailExtractors;
using CirclesLand.BlockchainIndexer.Persistence;
using CirclesLand.BlockchainIndexer.Sources;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using CirclesLand.BlockchainIndexer.Util;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;
using Npgsql;
using Prometheus;

namespace CirclesLand.BlockchainIndexer
{
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

        private Source<(int TotalTransactionsInBlock, HexBigInteger Timestamp, Transaction Transaction,
            TransactionReceipt Receipt), NotUsed>? _externalSource;

        private static readonly Counter RoundsTotal =
            Metrics.CreateCounter("indexer_main_loop_rounds_total", 
                "Totals of all processed rounds so far", "state");

        private static readonly Counter BatchesTotal =
            Metrics.CreateCounter("indexer_main_loop_batches_total", 
                "Totals of all processed batches so far", "state");
        
        private static readonly Counter BlocksTotal =
            Metrics.CreateCounter("indexer_main_loop_blocks_total", 
                "Totals of all processed blocks so far", "state");

        private static readonly Counter TransactionsTotal =
            Metrics.CreateCounter("indexer_main_loop_transactions_total",
                "Totals of all processed transactions so far", "state");

        private static readonly Counter EventsTotal =
            Metrics.CreateCounter("indexer_main_loop_events_total",
                "Totals of all processed events so far", "type");

        private static readonly Counter SafeOwnershipChecksTotal =
            Metrics.CreateCounter("indexer_main_loop_safe_ownership_checks_total",
                "Totals of all processed events so far");

        private static readonly Counter ReceiptsTotal =
            Metrics.CreateCounter("indexer_main_loop_receipts_total", 
                "Totals of all processed receipts so far", "state");
        
        private static readonly Gauge LastBlock = Metrics
            .CreateGauge("indexer_main_loop_last_processed_block_no", "The block number of the last processed block. Should only every go up. Down indicates something went wrong.", "state");

        public async Task Run(CancellationToken cancellationToken)
        {
            var system = ActorSystem.Create("system");
            var materializer = system.Materializer();
            var instanceContext = new InstanceContext();

            while (!cancellationToken.IsCancellationRequested)
            {
                await IndexerMain(cancellationToken, instanceContext, materializer);
            }
        }

        private async Task IndexerMain(CancellationToken cancellationToken, InstanceContext instanceContext,
            ActorMaterializer materializer)
        {
            var roundContext = instanceContext.CreateRoundContext();
            RoundsTotal.WithLabels("started").Inc();

            try
            {
                var roundStartsIn = roundContext.StartAt - DateTime.Now;
                if (roundStartsIn.TotalMilliseconds > 0)
                {
                    Logger.Log($"Round {roundContext.RoundNo} starting at {roundContext.StartAt} ..");
                    await Task.Delay(roundStartsIn, cancellationToken);
                }

                roundContext.Log($"Round {roundContext.RoundNo} started at {DateTime.Now}.");
                
                if (ReorgSource.BlockReorgsSharedState.Count > 0)
                {
                    CleanupAfterReorg();
                    ReorgSource.BlockReorgsSharedState.Clear();
                }
                else
                {
                    roundContext.Log($" Importing from staging tables ..");
                    ImportProcedure.ImportFromStaging(roundContext.Connection, Settings.BulkFlushTimeoutInSeconds);
                }

                roundContext.Log($"Finding the last persisted block ..");
                var lastPersistedBlock = roundContext.GetLastValidBlock();
                LastBlock.WithLabels("at_round_start_imported").Set(lastPersistedBlock);
                roundContext.Log($"Last persisted block: {lastPersistedBlock}");

                roundContext.Log($"Finding the latest blockchain block ..");
                
                var currentBlock = await  roundContext.Start(lastPersistedBlock);
                roundContext.Log($"Latest blockchain block: {currentBlock.Value}");
                LastBlock.WithLabels("at_round_start_live").Set(currentBlock.ToLong());
                
                // roundContext.Log($"Checking for reorgs in the last {Settings.UseBulkSourceThreshold} blocks ...");
                //
                // using (var connection = new NpgsqlConnection(Settings.ConnectionString))
                // {
                //     connection.Open();
                //     var web3 = new Web3(Settings.RpcEndpointUrl);
                //     var lastReorgAt = await ReorgSource.CheckForReorgsInLastBlocks(connection, web3, lastPersistedBlock, Settings.UseBulkSourceThreshold);
                //     if (lastReorgAt < long.MaxValue)
                //     {
                //         roundContext.Log($"Reorg at: {lastReorgAt}");
                //     }
                // }
                
                Source<(int TotalTransactionsInBlock, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt Receipt), NotUsed>? activeSource;

                int flushEveryNthBatch;
                if (_externalSource == null)
                {
                    var source = await DetermineSource(roundContext, lastPersistedBlock,
                        currentBlock);
                    
                    var reorgSource = roundContext.SourceFactory.CreateReorgSource();
                    var combinedSource1 = Source.Combine(reorgSource, source, i => new Merge<HexBigInteger>(i));
                    
                    flushEveryNthBatch = Mode == IndexerMode.CatchUp 
                        ? Settings.BulkFlushInterval 
                        : Settings.SerialFlushInterval;
                    
                    activeSource = TransactionAndReceiptSource(
                        Mode == IndexerMode.CatchUp
                            ? Source.Combine(source, GapSource.Create(120000, Settings.ConnectionString, true), i => new Merge<HexBigInteger>(i))
                            : Source.Combine(combinedSource1, GapSource.Create(120000, Settings.ConnectionString), i => new Merge<HexBigInteger>(i)), 
                        roundContext, 
                        flushEveryNthBatch);
                }
                else
                {
                    flushEveryNthBatch = 10;
                    activeSource = _externalSource;
                }
                
                if (activeSource == null)
                {
                    throw new Exception("No stream source");
                }

                await RunStream(materializer, activeSource, roundContext, flushEveryNthBatch);

                Logger.Log($"Completed the stream. Running last import of this round ..");
                CompleteBatch(flushEveryNthBatch, roundContext, true, Array.Empty<(int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction, TransactionReceipt? Receipt, TransactionClass Classification, IDetail[] Details)>());
                
                RoundsTotal.WithLabels("finished").Inc();
            }
            catch (Exception ex)
            {
                RoundsTotal.WithLabels("failed").Inc();
                roundContext.OnError(ex);
            }
        }

        private Source<(int TotalTransactionsInBlock, HexBigInteger Timestamp, Transaction Transaction, TransactionReceipt Receipt),NotUsed> 
            TransactionAndReceiptSource(Source<HexBigInteger, NotUsed> source, RoundContext roundContext,
            int flushEveryNthBatch)
        {
            return source
                .Select(o =>
                {
                    BlocksTotal.WithLabels("known").Inc();
                    LastBlock.WithLabels("from_source").Set(o.ToUlong());

                    if (ReorgSource.BlockReorgsSharedState.Count > 0)
                    {
                        var color = Console.ForegroundColor;
                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine($"Restarting ..");
                        Console.ForegroundColor = color;

                        throw new Exception("A reorg occurred and the round needs to be restarted.");
                    }

                    HealthService.ReportStartImportBlock(o.ToLong());
                    BlockTracker.AddRequested(roundContext.Connection, o.ToLong());
                    return o;
                })
                // Get the full block with all transactions
                .SelectAsync(Settings.MaxParallelBlockDownloads, currentBlockNo =>
                {
                    BlocksTotal.WithLabels("download_started").Inc();

                    return roundContext.Web3.Eth.Blocks
                        .GetBlockWithTransactionsByNumber
                        .SendRequestAsync(currentBlockNo);
                })
                .Buffer(Mode == IndexerMode.CatchUp ? Settings.MaxDownloadedBlockBufferSize : 1,
                    OverflowStrategy.Backpressure)
                // Bundle the every transaction in a block with the block timestamp and send it downstream
                .SelectMany(block =>
                {
                    BlocksTotal.WithLabels("download_finished").Inc();

                    var t = block.Transactions.ToArray();
                    TransactionsTotal.WithLabels("downloaded").Inc(t.Length);

                    if (t.Length == 0)
                    {
                        BlockTracker.InsertEmptyBlock(roundContext.Connection, block);
                        if (Mode == IndexerMode.Live || Mode == IndexerMode.Polling)
                        {
                            CompleteBatch(flushEveryNthBatch, roundContext, false,
                                Array.Empty<(int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp,
                                    Transaction Transaction, TransactionReceipt? Receipt, TransactionClass
                                    Classification, IDetail[] Details)>());
                        }
                    }

                    var transactions = t
                        .Select(o => (
                            TotalTransactionsInBlock: t.Length,
                            Timestamp: block.Timestamp,
                            Transaction: o))
                        .ToArray();

                    return transactions;
                })
                .Buffer(Mode == IndexerMode.CatchUp ? Settings.MaxDownloadedTransactionsBufferSize : 1,
                    OverflowStrategy.Backpressure)
                // Add the receipts for every transaction
                .SelectAsync(Settings.MaxParallelReceiptDownloads, async timestampAndTransaction =>
                {
                    ReceiptsTotal.WithLabels("download_started").Inc();

                    var receipt = await roundContext.Web3.Eth.Transactions.GetTransactionReceipt
                        .SendRequestAsync(
                            timestampAndTransaction.Transaction.TransactionHash);

                    ReceiptsTotal.WithLabels("download_finished").Inc();

                    return (
                        TotalTransactionsInBlock: timestampAndTransaction.TotalTransactionsInBlock,
                        Timestamp: timestampAndTransaction.Timestamp,
                        Transaction: timestampAndTransaction.Transaction,
                        Receipt: receipt
                    );
                })
                .Buffer(Mode == IndexerMode.CatchUp ? Settings.MaxDownloadedReceiptsBufferSize : 1,
                    OverflowStrategy.Backpressure);
        }

        private static void CleanupAfterReorg()
        {
            var lastReorgBlock = ReorgSource.BlockReorgsSharedState.Min();

            var color = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Magenta;
            LastBlock.WithLabels("reorg_at").Set(lastReorgBlock);
            Console.WriteLine($"Processing reorg starting at {lastReorgBlock} ...");
            Console.ForegroundColor = color;

            using var connection = new NpgsqlConnection(Settings.ConnectionString);
            connection.Open();

            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine($"Deleting all data >= block {lastReorgBlock} ...");
            Console.ForegroundColor = color;

            var tx = connection.BeginTransaction(IsolationLevel.Serializable);
            new NpgsqlCommand($"delete from crc_hub_transfer_2 where block_number >= {lastReorgBlock}", connection, tx)
                .ExecuteNonQuery();
            new NpgsqlCommand($"delete from crc_organisation_signup_2 where block_number >= {lastReorgBlock}", connection, tx)
                .ExecuteNonQuery();
            new NpgsqlCommand($"delete from crc_signup_2 where block_number >= {lastReorgBlock}", connection, tx)
                .ExecuteNonQuery();
            new NpgsqlCommand($"delete from crc_trust_2 where block_number >= {lastReorgBlock}", connection, tx)
                .ExecuteNonQuery();
            new NpgsqlCommand($"delete from erc20_transfer_2 where block_number >= {lastReorgBlock}", connection, tx)
                .ExecuteNonQuery();
            new NpgsqlCommand($"delete from eth_transfer_2 where block_number >= {lastReorgBlock}", connection, tx)
                .ExecuteNonQuery();
            new NpgsqlCommand($"delete from gnosis_safe_eth_transfer_2 where block_number >= {lastReorgBlock}", connection, tx)
                .ExecuteNonQuery();
            new NpgsqlCommand($"delete from transaction_2 where block_number >= {lastReorgBlock}", connection, tx)
                .ExecuteNonQuery();
            new NpgsqlCommand($"delete from block where number >= {lastReorgBlock}", connection, tx).ExecuteNonQuery();

            Console.WriteLine($"Deleting the contents of all staging tables ...");
            new NpgsqlCommand("delete from _crc_hub_transfer_staging;", connection, tx).ExecuteNonQuery();
            new NpgsqlCommand("delete from _crc_organisation_signup_staging;", connection, tx).ExecuteNonQuery();
            new NpgsqlCommand("delete from _crc_signup_staging;", connection, tx).ExecuteNonQuery();
            new NpgsqlCommand("delete from _crc_trust_staging;", connection, tx).ExecuteNonQuery();
            new NpgsqlCommand("delete from _erc20_transfer_staging;", connection, tx).ExecuteNonQuery();
            new NpgsqlCommand("delete from _eth_transfer_staging;", connection, tx).ExecuteNonQuery();
            new NpgsqlCommand("delete from _gnosis_safe_eth_transfer_staging;", connection, tx).ExecuteNonQuery();
            new NpgsqlCommand("delete from _transaction_staging;", connection, tx).ExecuteNonQuery();

            tx.Commit();
            connection.Close();

            ReorgSource.BlockReorgsSharedState.Clear();
        }

        private async Task RunStream(ActorMaterializer materializer, 
            Source<(int TotalTransactionsInBlock, HexBigInteger Timestamp, Transaction Transaction, TransactionReceipt Receipt),NotUsed> source, 
            RoundContext roundContext,
            int flushEveryNthBatch)
        {
                // Classify all transactions
                await source.Select(transactionAndReceipt =>
                {
                    var classification = transactionAndReceipt.Receipt == null
                        ? TransactionClass.Unknown
                        : TransactionClassifier.Classify(
                            transactionAndReceipt.Transaction,
                            transactionAndReceipt.Receipt,
                            null);

                    if (classification.HasFlag(TransactionClass.CrcSignup)) {
                        EventsTotal.WithLabels("crc_signup").Inc();
                    }
                    if (classification.HasFlag(TransactionClass.CrcTrust)) {
                        EventsTotal.WithLabels("crc_trust").Inc();
                    }
                    if (classification.HasFlag(TransactionClass.Erc20Transfer)) {
                        EventsTotal.WithLabels("erc20_transfer").Inc();
                    }
                    if (classification.HasFlag(TransactionClass.CrcHubTransfer)) {
                        EventsTotal.WithLabels("crc_hub_transfer").Inc();
                    }
                    if (classification.HasFlag(TransactionClass.CrcOrganisationSignup)) {
                        EventsTotal.WithLabels("crc_organization_signup").Inc();
                    }
                    if (classification.HasFlag(TransactionClass.EoaEthTransfer)) {
                        EventsTotal.WithLabels("eoa_eth_transfer").Inc();
                    }
                    if (classification.HasFlag(TransactionClass.SafeEthTransfer)) {
                        EventsTotal.WithLabels("safe_eth_transfer").Inc();
                    }
                    if (classification.HasFlag(TransactionClass.Unknown)) {
                        EventsTotal.WithLabels("other").Inc();
                    }

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
                    var extractedDetails = classifiedTransactions.Receipt != null
                        ? TransactionDetailExtractor.Extract(
                                classifiedTransactions.Classification,
                                classifiedTransactions.Transaction,
                                classifiedTransactions.Receipt)
                            .ToArray()
                        : new IDetail[] { };

                    // For every CrcSignup-event check who the owner is
                    var signups = extractedDetails
                        .Where(o => o is CrcSignup)
                        .Cast<CrcSignup>();

                    foreach (var signup in signups)
                    {
                        SafeOwnershipChecksTotal.Inc();
                        
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
                        try
                        {
                            SafeOwnershipChecksTotal.Inc();
                            
                            var contract = roundContext.Web3.Eth.GetContract(
                                GnosisSafeABI.Json, organisationSignup.Organization);
                            var function = contract.GetFunction("getOwners");
                            var owners = (await function.CallAsync<List<string>>()) ?? new List<string>();
                            organisationSignup.Owners = owners.Select(o => o.ToLower()).ToArray();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message + "\n" + ex.StackTrace);
                        }
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
                .Buffer(Mode == IndexerMode.CatchUp ? Settings.MaxWriteToStagingBatchBufferSize : 10, OverflowStrategy.Backpressure)
                .GroupedWithin(Settings.WriteToStagingBatchSize, TimeSpan.FromMilliseconds(Mode == IndexerMode.CatchUp ? Settings.WriteToStagingBatchMaxIntervalInSeconds * 1000 : 500))
                .RunForeach(transactionsWithExtractedDetails =>
                {
                    BatchesTotal.WithLabels("started").Inc();
                    
                    roundContext.Log($" Writing batch to staging tables ..");

                    var txArr = transactionsWithExtractedDetails.ToArray();

                    TransactionsWriter.WriteTransactions(
                        roundContext.Connection,
                        txArr);
                    
                    CompleteBatch(flushEveryNthBatch, roundContext, false, txArr);
                    HealthService.ReportCompleteBatch(txArr.Max(o => o.Transaction.BlockNumber.ToLong()));
                    
                    BatchesTotal.WithLabels("completed").Inc();
                }, materializer);
        }

        private async Task<Source<HexBigInteger, NotUsed>> DetermineSource(RoundContext roundContext, long lastPersistedBlock,
            HexBigInteger currentBlock)
        {
            var delta = currentBlock.Value - lastPersistedBlock;
            Source<HexBigInteger, NotUsed> source;
            if (delta > Settings.UseBulkSourceThreshold)
            {
                roundContext.Log($"Found {delta} blocks to catch up. Using the 'BulkSource'.");
                Mode = IndexerMode.CatchUp;

                source = roundContext.SourceFactory.CreateBulkSource(
                    new HexBigInteger(lastPersistedBlock)
                    , currentBlock);
            }
            else
            {
                // roundContext.Log($"Found {delta} blocks to catch up. Using the 'Polling' source.");
                // Mode = IndexerMode.Polling;
                // source = roundContext.SourceFactory.CreatePollingSource();

                roundContext.Log($"Found {delta} blocks to catch up. Using the 'Live' source.");
                Mode = IndexerMode.Live;
                source = await roundContext.SourceFactory.CreateLiveSource(lastPersistedBlock);
            }

            return source;
        }

        private void CompleteBatch(int flushEveryNthBatch, RoundContext roundContext, bool forceFlush, (int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction, TransactionReceipt? Receipt, TransactionClass Classification, IDetail[] Details)[] transactionsWithExtractedDetails)
        {
            string[] writtenTransactions = { };
            if (Statistics.TotalProcessedBatches % flushEveryNthBatch == 0 || forceFlush)
            {
                roundContext.Log($" Importing from staging tables ..");
                ImportProcedure.ImportFromStaging(roundContext.Connection
                    , Mode == IndexerMode.CatchUp
                        ? Settings.BulkFlushTimeoutInSeconds
                        : Settings.SerialFlushTimeoutInSeconds);

                roundContext.Log(" Cleaning staging tables ..");
                writtenTransactions = StagingTables.CleanImported(roundContext.Connection);
                
                TransactionsTotal.WithLabels("imported").Inc(writtenTransactions.Length);
                
                var processedBlocks = new HashSet<long>();
                transactionsWithExtractedDetails.ForEach(o => processedBlocks.Add(o.Transaction.BlockNumber.ToLong()));
                processedBlocks.ForEach(o =>
                {
                    Statistics.TrackBlockWritten(o);
                });

                if (processedBlocks.Count > 0)
                {
                    var lastProcessedBlock = processedBlocks.Max(o => o);
                    LastBlock.WithLabels("imported").Set(lastProcessedBlock);
                }
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
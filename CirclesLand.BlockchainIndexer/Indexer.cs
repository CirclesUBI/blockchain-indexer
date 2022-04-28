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

namespace CirclesLand.BlockchainIndexer
{
    public record BlockWithTransaction (
        long Block, 
        int TotalTransactionsInBlock, 
        HexBigInteger Timestamp,
        Transaction Transactions);
    
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

        private Source<(int TotalTransactionsInBlock, HexBigInteger Timestamp, Transaction Transaction,
            TransactionReceipt Receipt), NotUsed>? _externalSource;

        public async Task Run(Source<(int TotalTransactionsInBlock, HexBigInteger Timestamp, Transaction Transaction,
            TransactionReceipt Receipt), NotUsed>? externalSource)
        {
            _externalSource = externalSource;
            
            var system = ActorSystem.Create("system");
            var materializer = system.Materializer();
            var instanceContext = new InstanceContext();

            await IndexerMain(CancellationToken.None, instanceContext, materializer);
        }
        
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

                Source<(int TotalTransactionsInBlock, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt Receipt), NotUsed>? activeSource;

                int flushEveryNthBatch;
                if (_externalSource == null)
                {
                    var source = await DetermineSource(roundContext, lastPersistedBlock,
                        currentBlock);
                    
                    var reorgSource = roundContext.SourceFactory.CreateReorgSource();
                    var combinedSource = Source.Combine(reorgSource, source, i => new Merge<HexBigInteger>(i));

                    flushEveryNthBatch = Mode == IndexerMode.CatchUp 
                        ? Settings.BulkFlushInterval 
                        : Settings.SerialFlushInterval;
                    
                    activeSource = TransactionAndReceiptSource(
                        Mode == IndexerMode.CatchUp 
                            ? source
                            : combinedSource, 
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
                CompleteBatch(flushEveryNthBatch, roundContext, true);
            }
            catch (Exception ex)
            {
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
                    if (ReorgSource.BlockReorgsSharedState.Contains(o.ToLong()))
                    {
                        var color = Console.ForegroundColor;
                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine($"Processing reorg starting at {o.ToLong()} ...");
                        Console.ForegroundColor = color;

                        using var connection = new NpgsqlConnection(Settings.ConnectionString);
                        connection.Open();
                        
                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine($"Deleting all data >= block ${o.ToLong()} ...");
                        Console.ForegroundColor = color;
                        
                        var tx = connection.BeginTransaction();
                        new NpgsqlCommand($"delete from crc_hub_transfer_2 where block_number >= {o.ToLong()}", connection, tx).ExecuteNonQuery();
                        new NpgsqlCommand($"delete from crc_organisation_signup_2 where block_number >= {o.ToLong()}", connection, tx).ExecuteNonQuery();
                        new NpgsqlCommand($"delete from crc_signup_2 where block_number >= {o.ToLong()}", connection, tx).ExecuteNonQuery();
                        new NpgsqlCommand($"delete from crc_trust_2 where block_number >= {o.ToLong()}", connection, tx).ExecuteNonQuery();
                        new NpgsqlCommand($"delete from erc20_transfer_2 where block_number >= {o.ToLong()}", connection, tx).ExecuteNonQuery();
                        new NpgsqlCommand($"delete from eth_transfer_2 where block_number >= {o.ToLong()}", connection, tx).ExecuteNonQuery();
                        new NpgsqlCommand($"delete from gnosis_safe_eth_transfer_2 where block_number >= {o.ToLong()}", connection, tx).ExecuteNonQuery();
                        new NpgsqlCommand($"delete from transaction_2 where block_number >= {o.ToLong()}", connection, tx).ExecuteNonQuery();
                        new NpgsqlCommand($"delete from block where number >= {o.ToLong()}", connection, tx).ExecuteNonQuery();

                        tx.Commit();
                        connection.Close();
                        
                        ReorgSource.BlockReorgsSharedState.Clear();
                        
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
                        CompleteBatch(flushEveryNthBatch, roundContext);
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
                .Buffer(Settings.MaxDownloadedReceiptsBufferSize, OverflowStrategy.Backpressure);
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
                        var owners = (await function.CallAsync<List<string>>()) ?? new List<string>();
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

                    CompleteBatch(flushEveryNthBatch, roundContext);
                    HealthService.ReportCompleteBatch(txArr.Max(o => o.Transaction.BlockNumber.ToLong()));
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
                source = await roundContext.SourceFactory.CreateLiveSource();
            }

            return source;
        }

        private void CompleteBatch(int flushEveryNthBatch, RoundContext roundContext, bool flush = false)
        {
            string[] writtenTransactions = { };
            if (Statistics.TotalProcessedBatches % flushEveryNthBatch == 0 || flush)
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
            
            if (Statistics.TotalProcessedBatches % flushEveryNthBatch == 0)
            {
                Statistics.Print();
            }
        }
    }
}
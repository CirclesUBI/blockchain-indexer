using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using CirclesLand.BlockchainIndexer.DetailExtractors;
using CirclesLand.BlockchainIndexer.Sources;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using CirclesLand.BlockchainIndexer.Util;
using Dapper;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;
using Nethereum.Web3;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;

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


        // Total Stats:
        static long totalBlocksDownloaded = 0;
        static long totalProcessedTransactions = 0;
        static long totalProcessedErc20Transfers = 0;
        static long totalProcessedBlocks = 0;
        static long totalDownloadedTransactions = 0;
        static long totalErrors = 0;
        static long totalRounds = 0;
        private static long totalProcessedHubTransfers;
        private static long totalProcessedSignups;
        private static long totalProcessedOrgansationSignups;
        private static long totalProcessedEthTransfers;
        private static long totalProcessedTrusts;
        private static long totalProcessedSafeEthTransfers;

        private static long ticks = 0L;
        private static string blockTableName = $"";
        private static string transactionTableName = $"";
        private static string hubTransferTableName = $"";
        private static string organisationSignupTableName = $"";
        private static string signupTableName = $"";
        private static string trustTableName = $"";
        private static string erc20TransferTableName = $"";
        private static string ethTransferTableName = $"";
        private static string gnosisSafeEthTransferTableName = $"";
        private static string createTableSql = $@"";

        private static void GenerateTableNames()
        {
            totalBlocksDownloaded = 0;
            totalProcessedTransactions = 0;
            totalProcessedErc20Transfers = 0;
            totalProcessedBlocks = 0;
            totalDownloadedTransactions = 0;

            ticks = DateTime.Now.Ticks;
            blockTableName = $"_block_staging"; //_{ticks}";
            transactionTableName = $"_transaction_staging"; //_{ticks}";
            hubTransferTableName = $"_crc_hub_transfer_staging"; //_{ticks}";
            organisationSignupTableName = $"_crc_organisation_signup_staging"; //_{ticks}";
            signupTableName = $"_crc_signup_staging"; //_{ticks}";
            trustTableName = $"_crc_trust_staging"; //_{ticks}";
            erc20TransferTableName = $"_erc20_transfer_staging"; //_{ticks}";
            ethTransferTableName = $"_eth_transfer_staging"; //_{ticks}";
            gnosisSafeEthTransferTableName = $"_gnosis_safe_eth_transfer_staging"; //_{ticks}";

            createTableSql = $@"                        
                select number, hash, timestamp, total_transaction_count, null::timestamp as selected_at, null::timestamp as imported_at, null::boolean as already_available
                into {blockTableName}
                from block
                limit 0;

create index ix_block_staging_selected_at on _block_staging(number) include (selected_at);

                select hash, index, timestamp, block_number, ""from"", ""to"", value::text
                into {hubTransferTableName}
                from crc_hub_transfer_2
                limit 0;

create index ix_crc_hub_transfer_staging_hash on _crc_hub_transfer_staging(hash) include (block_number);

                select *
                into {organisationSignupTableName}
                from crc_organisation_signup_2
                limit 0;

create index ix_crc_organisation_signup_staging_hash on _crc_organisation_signup_staging(hash) include (block_number);

                select *
                into {signupTableName}
                from crc_signup_2
                limit 0;

create index ix_crc_signup_staging_hash on _crc_signup_staging(hash) include (block_number);

                select *
                into {trustTableName}
                from crc_trust_2
                limit 0;

create index ix_crc_trust_staging_hash on _crc_trust_staging(hash) include (block_number);

                select hash, index, timestamp, block_number, ""from"", ""to"", token, value::text
                into {erc20TransferTableName}
                from erc20_transfer_2
                limit 0;

create index ix_erc20_transfer_staging_hash on _erc20_transfer_staging(hash) include (block_number);
create index ix_erc20_transfer_staging_from on _erc20_transfer_staging(""from"");
create index ix_erc20_transfer_staging_to on _erc20_transfer_staging(""to"");

                select hash, index, timestamp, block_number, ""from"", ""to"", value::text
                into {ethTransferTableName}
                from eth_transfer_2
                limit 0;

create index ix_eth_transfer_staging_hash on _eth_transfer_staging(hash) include (block_number);
create index ix_eth_transfer_staging_from on _eth_transfer_staging(""from"");
create index ix_eth_transfer_staging_to on _eth_transfer_staging(""to"");

                select hash, index, timestamp, block_number, initiator, ""from"", ""to"", value::text
                into {gnosisSafeEthTransferTableName}
                from gnosis_safe_eth_transfer_2
                limit 0;

create index ix_gnosis_safe_eth_transfer_staging_hash on _gnosis_safe_eth_transfer_staging(hash) include (block_number);
create index ix_gnosis_safe_eth_transfer_staging_from on _gnosis_safe_eth_transfer_staging(""from"");
create index ix_gnosis_safe_eth_transfer_staging_to on _gnosis_safe_eth_transfer_staging(""to"");

                select block_number, ""from"", ""to"", hash, index, timestamp, value::text, input, nonce, type, classification
                into {transactionTableName}
                from transaction_2
                limit 0;

create index ix_transaction_staging_hash on _transaction_staging(hash) include (block_number);
            ";
        }

        public async Task Run(Action<long> onBlockWritten, int maxBlockDownloads = 24, int maxReceiptDownloads = 96)
        {
            HexBigInteger currentlyWritingBlock = new(0);
            var clientId = Guid.NewGuid().ToString("N");
            var startedAt = DateTime.Now;
            var system = ActorSystem.Create("system");
            var materializer = system.Materializer();
            var errorRestartPenaltyInMs = 0;
            const int maxErrorRestartPenalty = 3 * 60 * 1000;
            var currentErrorRestarts = 0;
            Exception? lastRoundError = null;
            const int batchSize = 2000;
            const int batchInterval = 2;

            void ResetErrorPenalty()
            {
                lastRoundError = null;
                currentErrorRestarts = 0;
                errorRestartPenaltyInMs = 0;
            }

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

                    Logger.Log($"Creating the staging tables ..");
                    startedAt = DateTime.Now;
                    GenerateTableNames();
                    try
                    {
                        writerConnection.Execute(createTableSql);
                    }
                    catch (Exception)
                    {
                        // ignored
                    }

                    if (lastRoundError != null)
                    {
                        Logger.Log("Clearing the staging tables after an error ..");
                        writerConnection.Execute(@"
                            truncate _block_staging;
                            truncate _crc_hub_transfer_staging;
                            truncate _crc_organisation_signup_staging;
                            truncate _crc_signup_staging;
                            truncate _erc20_transfer_staging;
                            truncate _eth_transfer_staging;
                            truncate _gnosis_safe_eth_transfer_staging;
                            truncate _transaction_staging;
                        ");
                        Logger.Log("Staging tables have been cleared.");
                    }

                    Source<HexBigInteger, NotUsed> source;
                    var web3 = new Web3(_rpcEndpointUrl);

                    Logger.Log("Getting the currently latest block number ..");
                    var currentBlock = await web3.Eth.Blocks
                        .GetBlockNumber.SendRequestAsync();

                    Logger.Log($"The current block is {currentBlock}.");

                    var lastKnownBlock = writerConnection.QuerySingleOrDefault<long?>(
                        @"with a as (
                                select distinct block_no
                                from requested_blocks
                                order by block_no
                            ), b as (
                                select distinct number
                                from block
                                order by number
                            ), c as (
                                select a.block_no as requested, b.number as actual
                                from a
                                left join b on a.block_no = b.number
                                order by a.block_no
                            )
                            select min(c.requested) - 1 as last_correctly_imported_block
                            from c
                            where actual is null;") ?? 12529458;

                    _firstNewBlock = lastKnownBlock - 1;

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

                    int healthCheckCounter = 0;
                    int committingUntilRounds = 0;
                    
                    Exception? healthCheckError = null;

                    await source
                        .Select(o =>
                        {
                            writerConnection.Execute($@"
                                    insert into requested_blocks (block_no)
                                    values (@number) on conflict do nothing;",
                                new
                                {
                                    number = o.ToLong()
                                });
                            return o;
                        })
                        // Get the full block with all transactions
                        .SelectAsync(maxBlockDownloads, currentBlockNo =>
                            web3.Eth.Blocks
                                .GetBlockWithTransactionsByNumber
                                .SendRequestAsync(currentBlockNo))
                        .Buffer(maxBlockDownloads, OverflowStrategy.Backpressure)
                        // Bundle the every transaction in a block with the block timestamp and send it downstream
                        .SelectMany(block =>
                        {
                            Interlocked.Increment(ref totalBlocksDownloaded);

                            var t = block.Transactions.ToArray();
                            Interlocked.Add(ref totalTransactionsDownloaded, t.Length);

                            if (t.Length == 0)
                            {
                                var blockTimestamp = block.Timestamp.ToLong();
                                var blockTimestampDateTime =
                                    DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                                writerConnection.Execute($@"
                                    insert into _block_staging (number, hash, timestamp, total_transaction_count)
                                    values (@number, @hash, @timestamp, 0);",
                                    new
                                    {
                                        number = block.Number.ToLong(),
                                        hash = block.BlockHash,
                                        timestamp = blockTimestampDateTime
                                    });
                            }

                            var transactions = t
                                .Select(o => (
                                    TotalTransactionsInBlock: t.Length,
                                    Timestamp: block.Timestamp,
                                    Transaction: o))
                                .ToArray();

                            return transactions;
                        })
                        .Buffer(maxReceiptDownloads, OverflowStrategy.Backpressure)
                        // Add the receipts for every transaction
                        .SelectAsync(maxReceiptDownloads, async timestampAndTransaction =>
                        {
                            var receipt = await web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(
                                timestampAndTransaction.Transaction.TransactionHash);

                            Interlocked.Increment(ref totalReceiptsDownloaded);

                            return (
                                TotalTransactionsInBlock: timestampAndTransaction.TotalTransactionsInBlock,
                                Timestamp: timestampAndTransaction.Timestamp,
                                Transaction: timestampAndTransaction.Transaction,
                                Receipt: receipt
                            );
                        })
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
                                Details: extractedDetails
                            );
                        })
                        .GroupedWithin(batchSize, TimeSpan.FromSeconds(batchInterval))
                        .Buffer(5, OverflowStrategy.Backpressure)
                        .RunForeach(transactionsWithExtractedDetails =>
                        {
                            if (healthCheckError != null)
                            {
                                throw new Exception("The health check failed. See inner exception for details.",
                                    healthCheckError);
                            }

                            healthCheckCounter++;
                            
                            if ((healthCheckCounter == 15 || healthCheckCounter >= 30) && _committing == 0)
                            {
                                var maxUpstreamCachedTransactions = maxBlockDownloads * maxReceiptDownloads;
                                var maxPersistenceCachedTransactions = 5 * batchSize;
                                var maxCachedTransactions = (maxUpstreamCachedTransactions + maxPersistenceCachedTransactions) / 5;

                                Console.WriteLine($"Max tolerated backlog size (blocks): {maxCachedTransactions}");

                                var healthCheckSql =
                                    @$"with max_imported as (
                                    select max(number) as number from block
                                ), max_staging as (
                                    select max(number) as number from _block_staging
                                ), min_missing as (
                                    select min(block_no) -1 missing_block_begin
                                    from requested_blocks rb
                                    left join block b on rb.block_no = b.number and b.number < (select number from max_imported)
                                    where b.number is null
                                ), c as (
                                    select (select number from max_staging) - (select number from max_imported) as staging_distance
                                         , (select number from max_imported) - missing_block_begin              as imported_distance
                                    from min_missing
                                )
                                select *
                                from c
                                where c.imported_distance >= {(long) maxCachedTransactions}
                                   or c.staging_distance >= {(long) maxCachedTransactions};";

                                var cleanupSql = @"
                                delete from _gnosis_safe_eth_transfer_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                                delete from _eth_transfer_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                                delete from _erc20_transfer_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                                delete from _crc_trust_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                                delete from _crc_signup_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                                delete from _crc_organisation_signup_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                                delete from _crc_hub_transfer_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                                delete from _transaction_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                                delete from _block_staging where imported_at is not null;";

                                Task.Run(() =>
                                {
                                    Console.WriteLine("Performing health check ...");
                                    var hcs = DateTime.Now;
                                    using var healthCheckConnection = new NpgsqlConnection(_connectionString);
                                    healthCheckConnection.Open();

                                    try
                                    {
                                        var healthCheckResult = healthCheckConnection.QueryFirstOrDefault(
                                            healthCheckSql,
                                            null,
                                            null, 10);

                                        if (healthCheckResult != null)
                                        {
                                            // Will crash repeatedly if the gap is large until the gap is small enough
                                            // so that it doesn't trigger this exception anymore. O.k. for now (and maybe the future).
                                            healthCheckError =
                                                new Exception(
                                                    $"Found possible data corruption ('blocks' table contains a gap of at least {maxCachedTransactions} blocks):\n {JsonConvert.SerializeObject(healthCheckResult)}.");
                                            var hcsDuration = DateTime.Now - hcs;
                                            Console.WriteLine($"Unhealthy (check took {hcsDuration})");
                                        }
                                        else
                                        {
                                            var hcsDuration = DateTime.Now - hcs;
                                            Console.WriteLine($"HEALTHY (check took {hcsDuration})");

                                            if (healthCheckCounter == 30)
                                            {
                                                var t = healthCheckConnection.BeginTransaction(IsolationLevel
                                                    .ReadCommitted);
                                                Console.WriteLine("Performing cleanup of staging tables ...");
                                                healthCheckConnection.Execute(cleanupSql, null, t, 20);
                                                Console.WriteLine("Cleaned the staging tables.");
                                                t.Commit();
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine(ex.Message);
                                        Console.WriteLine(ex.StackTrace);
                                        healthCheckError = ex;
                                        throw;
                                    }
                                    
                                    if (healthCheckCounter >= 30)
                                    {
                                        healthCheckCounter = 0;
                                    }
                                });
                            }

                            if (_committing > 0)
                            {
                                committingUntilRounds++;
                            }
                            else
                            {
                                committingUntilRounds = 0;
                            }

                            var waitForCommitAfterNthRound = 7;
                            if (committingUntilRounds >= waitForCommitAfterNthRound && commitTask != null)
                            {
                                var timeoutIn = TimeSpan.FromSeconds(45);
                                var timeout = DateTime.Now + timeoutIn;
                                Console.WriteLine($".. Backpressure because the commit didn't finish within " +
                                                  $"{batchInterval * waitForCommitAfterNthRound} seconds " +
                                                  $"or {batchSize * waitForCommitAfterNthRound} new incoming " +
                                                  $"transactions ... (waiting max. {timeoutIn})");
                                commitTask.Wait(timeoutIn + TimeSpan.FromMilliseconds(10));
                                if (DateTime.Now > timeout)
                                {
                                    throw new Exception("Timed out while waiting for commit.");
                                }
                                Console.WriteLine("Backpressure resolved.");
                                committingUntilRounds = 0;
                            }

                            WriteTransactions(
                                writerConnection,
                                transactionsWithExtractedDetails,
                                startedAt);

                            ResetErrorPenalty();
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

        private static int _committing = 0;
        private static long totalTransactionsDownloaded;
        private static long totalReceiptsDownloaded;
        Task? commitTask = null;

        private void WriteTransactions(
            NpgsqlConnection writerConnection,
            IEnumerable<(
                int TotalTransactionsInBlock,
                string TxHash,
                HexBigInteger Timestamp,
                Transaction Transaction,
                TransactionReceipt Receipt,
                TransactionClass Classification,
                IDetail[] Details
                )> transactionsWithExtractedDetails,
            DateTime startedAt)
        {
            var transactionsWithExtractedDetailsArr = transactionsWithExtractedDetails.ToArray();

            var blockList =
                new HashSet<(long BlockNumber, DateTime BlockTimestamp, string hash, int totalTransactionCount)>();

            foreach (var t in transactionsWithExtractedDetailsArr)
            {
                var blockTimestamp = t.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;
                blockList.Add((
                    BlockNumber: t.Transaction.BlockNumber.ToLong(),
                    BlockTimestamp: blockTimestampDateTime,
                    hash: t.Transaction.BlockHash,
                    totalTransactionCount: t.TotalTransactionsInBlock
                ));
            }

            WriteBlocks(writerConnection, blockTableName, blockList);

            var details =
                transactionsWithExtractedDetailsArr.SelectMany(transaction =>
                        transaction.Details.Select(detail => (transaction, detail)))
                    .ToArray();

            WriteTransactionRows(
                writerConnection,
                transactionsWithExtractedDetailsArr,
                transactionTableName);

            var hubTransfers =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.CrcHubTransfer) &&
                    o.detail is CrcHubTransfer);

            WriteHubTransfers(writerConnection, hubTransferTableName, hubTransfers);

            var organisationSignups =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.CrcOrganisationSignup) &&
                    o.detail is CrcOrganisationSignup);

            WriteOrganisationSignups(writerConnection, organisationSignupTableName, organisationSignups);

            var signups =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.CrcSignup) && o.detail is CrcSignup);

            WriteSignups(writerConnection, signupTableName, signups);

            var trusts =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.CrcTrust) && o.detail is CrcTrust);

            WriteTrusts(writerConnection, trustTableName, trusts);

            var erc20Transfers =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.Erc20Transfer) && o.detail is Erc20Transfer);

            WriteErc20Transfers(writerConnection, erc20TransferTableName, erc20Transfers);

            var ethTransfers =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.EoaEthTransfer) && o.detail is EthTransfer);

            WriteEthTransfers(writerConnection, ethTransferTableName, ethTransfers);

            var safeEthTransfers =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.SafeEthTransfer) &&
                    o.detail is GnosisSafeEthTransfer);

            WriteSafeEthTransfers(writerConnection, gnosisSafeEthTransferTableName, safeEthTransfers);


            if (_committing == 0)
            {
                Interlocked.Increment(ref _committing);
                
                commitTask = Task.Run(() =>
                {
                    Console.WriteLine("committing...");
                    using var importConnection = new NpgsqlConnection(_connectionString);
                    importConnection.Open();
                    using var transaction = importConnection.BeginTransaction(IsolationLevel.ReadCommitted);

                    try
                    {
                        importConnection.Execute("call import_from_staging_2();", null, transaction, 45);
                        transaction.Commit();
                        Console.WriteLine($"COMITTED");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.StackTrace);
                        transaction.Rollback();
                        throw;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref _committing);
                    }
                });
            }

            var elapsedTime = DateTime.Now - startedAt;
            var defaultColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(
                $"Downloaded {totalBlocksDownloaded} blocks @ {totalBlocksDownloaded / elapsedTime.TotalSeconds} " +
                $"blocks per second.");
            Console.WriteLine(
                $"Processed {totalProcessedTransactions} 'transactions' @ {totalProcessedTransactions / elapsedTime.TotalSeconds} "
                +
                $"transactions per second.");
            Console.WriteLine(
                $"Processed {totalProcessedHubTransfers} 'hub transfers' @ {totalProcessedHubTransfers / elapsedTime.TotalSeconds} "
                +
                $"transactions per second.");
            Console.WriteLine(
                $"Processed {totalProcessedOrgansationSignups} 'organisation signups' @ {totalProcessedOrgansationSignups / elapsedTime.TotalSeconds} " +
                $"transactions per second.");
            Console.WriteLine(
                $"Processed {totalProcessedSignups} 'signups' @ {totalProcessedSignups / elapsedTime.TotalSeconds} " +
                $"transactions per second.");
            Console.WriteLine(
                $"Processed {totalProcessedTrusts} 'trusts' @ {totalProcessedTrusts / elapsedTime.TotalSeconds} " +
                $"transactions per second.");
            Console.WriteLine(
                $"Processed {totalProcessedErc20Transfers} 'erc 20 transfers' @ {totalProcessedErc20Transfers / elapsedTime.TotalSeconds} " +
                $"transactions per second.");
            Console.WriteLine(
                $"Processed {totalProcessedEthTransfers} 'eth transfers' @ {totalProcessedEthTransfers / elapsedTime.TotalSeconds} "
                +
                $"transactions per second.");
            Console.WriteLine(
                $"Processed {totalProcessedSafeEthTransfers} 'safe eth transfers' @ {totalProcessedSafeEthTransfers / elapsedTime.TotalSeconds} " +
                $"transactions per second.");
            Console.ForegroundColor = defaultColor;
        }

        private static void WriteBlocks(NpgsqlConnection writerConnection,
            string? blockTableName,
            IEnumerable<(long BlockNumber, DateTime BlockTimestamp, string Hash, int TotalTransactionCount)>? blocks)
        {
            using var writer = writerConnection.BeginBinaryImport(
                @$"COPY {blockTableName} (
                                         number
                                        ,hash
                                        ,timestamp
                                        ,total_transaction_count
                                        ,selected_at
                                        ,imported_at
                                    ) FROM STDIN (FORMAT BINARY)");
            foreach (var d in blocks)
            {
                writer.StartRow();
                writer.Write(d.BlockNumber, NpgsqlDbType.Bigint);
                writer.Write(d.Hash, NpgsqlDbType.Text);
                writer.Write(d.BlockTimestamp, NpgsqlDbType.Timestamp);
                writer.Write(d.TotalTransactionCount, NpgsqlDbType.Integer);
                writer.Write(DBNull.Value, NpgsqlDbType.Timestamp);
                writer.Write(DBNull.Value, NpgsqlDbType.Timestamp);
            }

            writer.Complete();
        }

        private static void WriteSafeEthTransfers(NpgsqlConnection writerConnection,
            string? safeEthTransferTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>?
                safeEthTransfers)
        {
            using var writer = writerConnection.BeginBinaryImport(
                @$"COPY {safeEthTransferTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,initiator
                                        ,""from""
                                        ,""to""
                                        ,value
                                    ) FROM STDIN (FORMAT BINARY)");

            foreach (var d in safeEthTransfers)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                writer.StartRow();

                writer.Write(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                writer.Write((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                writer.Write(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                writer.Write((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                writer.Write(((GnosisSafeEthTransfer) d.detail).Initiator, NpgsqlDbType.Text);
                writer.Write(((GnosisSafeEthTransfer) d.detail).From, NpgsqlDbType.Text);
                writer.Write(((GnosisSafeEthTransfer) d.detail).To, NpgsqlDbType.Text);
                writer.Write(((GnosisSafeEthTransfer) d.detail).Value, NpgsqlDbType.Text);

                Interlocked.Increment(ref totalProcessedSafeEthTransfers);
            }

            writer.Complete();
        }

        private static void WriteEthTransfers(NpgsqlConnection writerConnection,
            string? ethTransferTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>?
                ethTransfers)
        {
            using var writer = writerConnection.BeginBinaryImport(
                @$"COPY {ethTransferTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,""from""
                                        ,""to""
                                        ,value
                                    ) FROM STDIN (FORMAT BINARY)");

            foreach (var d in ethTransfers)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                writer.StartRow();

                writer.Write(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                writer.Write((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                writer.Write(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                writer.Write((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                writer.Write(((EthTransfer) d.detail).From, NpgsqlDbType.Text);
                writer.Write(((EthTransfer) d.detail).To, NpgsqlDbType.Text);
                writer.Write(((EthTransfer) d.detail).Value, NpgsqlDbType.Text);

                Interlocked.Increment(ref totalProcessedEthTransfers);
            }

            writer.Complete();
        }

        private static void WriteTrusts(NpgsqlConnection writerConnection,
            string? trustTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>?
                trusts)
        {
            using var writer = writerConnection.BeginBinaryImport(
                @$"COPY {trustTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,address
                                        ,can_send_to
                                        ,""limit""
                                    ) FROM STDIN (FORMAT BINARY)");

            foreach (var d in trusts)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                writer.StartRow();

                writer.Write(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                writer.Write((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                writer.Write(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                writer.Write((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                writer.Write(((CrcTrust) d.detail).Address, NpgsqlDbType.Text);
                writer.Write(((CrcTrust) d.detail).CanSendTo, NpgsqlDbType.Text);
                writer.Write((long) ((CrcTrust) d.detail).Limit, NpgsqlDbType.Numeric);

                Interlocked.Increment(ref totalProcessedTrusts);
            }

            writer.Complete();
        }

        private static void WriteSignups(NpgsqlConnection writerConnection,
            string? signupsTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>?
                signups)
        {
            using var writer = writerConnection.BeginBinaryImport(
                @$"COPY {signupsTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,""user""
                                        ,token
                                    ) FROM STDIN (FORMAT BINARY)");

            foreach (var d in signups)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                writer.StartRow();

                writer.Write(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                writer.Write((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                writer.Write(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                writer.Write((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                writer.Write(((CrcSignup) d.detail).User, NpgsqlDbType.Text);
                writer.Write(((CrcSignup) d.detail).Token, NpgsqlDbType.Text);

                Interlocked.Increment(ref totalProcessedSignups);
            }

            writer.Complete();
        }

        private static void WriteOrganisationSignups(NpgsqlConnection writerConnection,
            string? organisationSignupsTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>?
                organisationSignups)
        {
            using var writer = writerConnection.BeginBinaryImport(
                @$"COPY {organisationSignupsTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,organisation
                                    ) FROM STDIN (FORMAT BINARY)");

            foreach (var d in organisationSignups)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                writer.StartRow();

                writer.Write(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                writer.Write((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                writer.Write(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                writer.Write((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                writer.Write(((CrcOrganisationSignup) d.detail).Organization, NpgsqlDbType.Text);

                Interlocked.Increment(ref totalProcessedOrgansationSignups);
            }

            writer.Complete();
        }

        private static void WriteHubTransfers(NpgsqlConnection writerConnection,
            string? hubTransfersTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>?
                hubTransfers)
        {
            using var writer = writerConnection.BeginBinaryImport(
                @$"COPY {hubTransfersTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,""from""
                                        ,""to""
                                        ,value
                                    ) FROM STDIN (FORMAT BINARY)");

            foreach (var d in hubTransfers)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                writer.StartRow();

                writer.Write(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                writer.Write((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                writer.Write(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                writer.Write((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                writer.Write(((CrcHubTransfer) d.detail).From, NpgsqlDbType.Text);
                writer.Write(((CrcHubTransfer) d.detail).To, NpgsqlDbType.Text);
                writer.Write(((CrcHubTransfer) d.detail).Value, NpgsqlDbType.Text);

                Interlocked.Increment(ref totalProcessedHubTransfers);
            }

            writer.Complete();
        }

        private static void WriteErc20Transfers(NpgsqlConnection writerConnection, string? erc20TransferTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>?
                erc20Transfers)
        {
            using var writer = writerConnection.BeginBinaryImport(
                @$"COPY {erc20TransferTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,""from""
                                        ,""to""
                                        ,token
                                        ,value
                                    ) FROM STDIN (FORMAT BINARY)");

            foreach (var d in erc20Transfers)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                writer.StartRow();

                writer.Write(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                writer.Write((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                writer.Write(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                writer.Write((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                writer.Write(((Erc20Transfer) d.detail).From, NpgsqlDbType.Text);
                writer.Write(((Erc20Transfer) d.detail).To, NpgsqlDbType.Text);
                writer.Write(((Erc20Transfer) d.detail).Token, NpgsqlDbType.Text);
                writer.Write(((Erc20Transfer) d.detail).Value, NpgsqlDbType.Text);

                Interlocked.Increment(ref totalProcessedErc20Transfers);
            }

            writer.Complete();
        }

        private static void WriteTransactionRows(NpgsqlConnection writerConnection,
            IEnumerable<(int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt Receipt, TransactionClass Classification, IDetail[] Details)>
                transactionsWithExtractedDetails,
            string? transactionTableName)
        {
            using var writer = writerConnection.BeginBinaryImport(
                @$"COPY {transactionTableName} (
                                         block_number
                                        ,""from""
                                        ,""to""
                                        ,hash
                                        ,index
                                        ,timestamp
                                        ,value
                                        ,input
                                        ,nonce
                                        ,type
                                        ,classification
                                    ) FROM STDIN (FORMAT BINARY)");

            foreach (var t in transactionsWithExtractedDetails)
            {
                var blockTimestamp = t.Timestamp.ToLong();
                var blockTimestampDateTime =
                    DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;
                var classificationArray = t.Classification.ToString()
                    .Split(",", StringSplitOptions.TrimEntries);

                writer.StartRow();

                writer.Write((long) t.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                writer.Write(t.Transaction.From, NpgsqlDbType.Text);
                writer.Write(t.Transaction.To, NpgsqlDbType.Text);
                writer.Write(t.Transaction.TransactionHash, NpgsqlDbType.Text);
                writer.Write((int) t.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                writer.Write(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                writer.Write(t.Transaction.Value.Value.ToString(), NpgsqlDbType.Text);
                writer.Write(t.Transaction.Input, NpgsqlDbType.Text);
                writer.Write(t.Transaction.Nonce.ToString(), NpgsqlDbType.Text);
                writer.Write(t.Transaction.Type.ToString(), NpgsqlDbType.Text);
                writer.Write(classificationArray, NpgsqlDbType.Array | NpgsqlDbType.Text);

                Interlocked.Increment(ref totalProcessedTransactions);
            }

            writer.Complete();
        }
    }
}
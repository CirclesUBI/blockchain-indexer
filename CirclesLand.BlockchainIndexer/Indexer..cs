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
            const int batchSize = 2500;

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

                    Source<HexBigInteger, NotUsed> source;
                    var web3 = new Web3(_rpcEndpointUrl);

                    Logger.Log("Getting the currently latest block number ..");
                    var currentBlock = await web3.Eth.Blocks
                        .GetBlockNumber.SendRequestAsync();

                    Logger.Log($"The current block is {currentBlock}.");

                    var lastKnownBlock = writerConnection.QuerySingleOrDefault<long?>(
                        @"with a as (
                                select block_number
                                from transaction_2 t
                                         join block b on t.block_number = b.number
                                where block_number >= 17907816
                                group by block_number, b.total_transaction_count
                                having count(t.hash) != b.total_transaction_count
                                order by block_number
                            ), b as (
                                select min(block_number) as block_number
                                from a
                            ), c as (
                                select max(number) as block_number
                                from block
                                union all
                                select block_number
                                from b
                                where b.block_number is not null
                            )
                            select min(block_number) from c;") ?? 12529458;

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
                        .GroupedWithin(batchSize, TimeSpan.FromSeconds(5))
                        .Buffer(20, OverflowStrategy.Backpressure)
                        .RunForeach(transactionsWithExtractedDetails =>
                        {
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

            var details =
                transactionsWithExtractedDetailsArr.SelectMany(transaction =>
                        transaction.Details.Select(detail => (transaction, detail)))
                    .ToArray();

            var blocks = details
                .Aggregate(ImmutableHashSet.Create<(DateTime, int, Transaction)>(),
                    (p, c) =>
                    {
                        var blockTimestamp = c.transaction.Timestamp.ToLong();
                        var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;
                        return p.Add(
                            (blockTimestampDateTime, c.transaction.TotalTransactionsInBlock,
                                c.transaction.Transaction));
                    })
                .Aggregate(
                    ImmutableHashSet
                        .Create<(long BlockNumber, DateTime BlockTimestamp, string hash, int totalTransactionCount)>(),
                    (p, c) => p.Add((c.Item3.BlockNumber.ToLong(), c.Item1, c.Item3.BlockHash, c.Item2)))
                .ToArray();

            WriteBlocks(writerConnection, blockTableName, blocks);

            WriteTransactionRows(
                writerConnection,
                transactionsWithExtractedDetails,
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
            //
            Task.Run(() =>
            {
                if (_committing > 0)
                {
                    return;
                }

                Interlocked.Increment(ref _committing);
                
                Console.WriteLine("committing...");
                using var importConnection = new NpgsqlConnection(_connectionString);
                importConnection.Open();
                using var transaction = importConnection.BeginTransaction(IsolationLevel.ReadCommitted);

                try
                {
                    importConnection.Execute("call import_from_staging();", null, transaction);
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
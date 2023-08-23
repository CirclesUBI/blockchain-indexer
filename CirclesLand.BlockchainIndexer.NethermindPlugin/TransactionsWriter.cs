using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.NethermindPlugin
{
    public class TransactionsWriter
    {
        public static string blockTableName = $"_block_staging";
        public static string transactionTableName = $"_transaction_staging";
        public static string hubTransferTableName = $"_crc_hub_transfer_staging";
        public static string organisationSignupTableName = $"_crc_organisation_signup_staging";
        public static string signupTableName = $"_crc_signup_staging";
        public static string trustTableName = $"_crc_trust_staging";
        public static string erc20TransferTableName = $"_erc20_transfer_staging";
        public static string ethTransferTableName = $"_eth_transfer_staging";
        public static string gnosisSafeEthTransferTableName = $"_gnosis_safe_eth_transfer_staging";

        public static async Task WriteTransactions(
            string writerConnectionString,
            IEnumerable<TransactionWithEvents> transactionsWithEvents)
        {
            // var transactionsWithExtractedDetailsArr = transactionsWithExtractedDetails.ToArray();

            var blockList =
                new HashSet<(long BlockNumber, DateTime BlockTimestamp, string hash, int totalTransactionCount)>();

            foreach (var transactionWithEvents in transactionsWithEvents)
            {
                var blockTimestamp = transactionWithEvents.TransactionWithReceipts.Block.Timestamp;
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds((long)blockTimestamp).UtcDateTime;
                blockList.Add((
                    BlockNumber: transactionWithEvents.TransactionWithReceipts.Block.Number,
                    BlockTimestamp: blockTimestampDateTime,
                    hash: transactionWithEvents.TransactionWithReceipts.Block.Hash.ToString(),
                    totalTransactionCount: transactionWithEvents.TransactionWithReceipts.Block.Transactions.Length
                ));
            }

            var _writerConnection = await GetDbConnection(writerConnectionString);
            BlockWriter.WriteBlocks(_writerConnection, blockTableName, blockList);

            var details =
                transactionsWithEvents.SelectMany(transaction => 
                        transaction.Events.Select(detail => 
                            (transaction.TransactionWithReceipts.Transaction, transaction.TransactionWithReceipts.Receipt, detail: detail.Detail)))
                    .ToArray();

            var promises = new List<Task>();

            promises.Add(StagingTables.WriteTransactionRows(
                _writerConnection,
                transactionTableName,
                transactionsWithEvents).ContinueWith(_ => _writerConnection.Close()));

            var hubTransfers =
                details.Where(o => o.detail is CrcHubTransfer).ToArray();

            if (hubTransfers.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteHubTransfers(writerConnection, hubTransferTableName, hubTransfers)
                    .ContinueWith(_ => writerConnection.Close()));
            }

            var organisationSignups =
                details.Where(o => o.detail is CrcOrganisationSignup).ToArray();

            if (organisationSignups.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteOrganisationSignups(writerConnection, organisationSignupTableName,
                    organisationSignups).ContinueWith(_ => writerConnection.Close()));
            }

            var signups =
                details.Where(o => o.detail is CrcSignup)
                    .ToArray();

            if (signups.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteSignups(writerConnection, signupTableName, signups)
                    .ContinueWith(_ => writerConnection.Close()));
            }

            var trusts =
                details.Where(o => o.detail is CrcTrust)
                    .ToArray();

            if (trusts.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteTrusts(writerConnection, trustTableName, trusts)
                    .ContinueWith(_ => writerConnection.Close()));
            }

            var erc20Transfers =
                details.Where(o => o.detail is Erc20Transfer)
                    .ToArray();

            if (erc20Transfers.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteErc20Transfers(writerConnection, erc20TransferTableName, erc20Transfers)
                    .ContinueWith(_ => writerConnection.Close()));
            }

            var ethTransfers =
                details.Where(o => o.detail is EthTransfer)
                    .ToArray();

            if (ethTransfers.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteEthTransfers(writerConnection, ethTransferTableName, ethTransfers)
                    .ContinueWith(_ => writerConnection.Close()));
            }

            var safeEthTransfers =
                details.Where(o => o.detail is GnosisSafeEthTransfer)
                    .ToArray();

            if (safeEthTransfers.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteSafeEthTransfers(writerConnection, gnosisSafeEthTransferTableName,
                    safeEthTransfers).ContinueWith(_ => writerConnection.Close()));
            }

            await Task.WhenAll(promises);
        }

        private static async Task<NpgsqlConnection> GetDbConnection(string writerConnectionString)
        {
            var writerConnection = new NpgsqlConnection(writerConnectionString);
            await writerConnection.OpenAsync();
            return writerConnection;
        }
    }
}
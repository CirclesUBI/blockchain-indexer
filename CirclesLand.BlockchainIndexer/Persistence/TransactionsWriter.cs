using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence
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
            IEnumerable<(
                int TotalTransactionsInBlock,
                string TxHash,
                HexBigInteger Timestamp,
                Transaction Transaction,
                TransactionReceipt? Receipt,
                TransactionClass Classification,
                IDetail[] Details
                )> transactionsWithExtractedDetails)
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

            var _writerConnection = await GetDbConnection(writerConnectionString);
            BlockWriter.WriteBlocks(_writerConnection, blockTableName, blockList);

            var details =
                transactionsWithExtractedDetailsArr.SelectMany(transaction =>
                        transaction.Details.Select(detail => (transaction, detail)))
                    .ToArray();

            var promises = new List<Task>();

            promises.Add(StagingTables.WriteTransactionRows(
                _writerConnection,
                transactionsWithExtractedDetailsArr,
                transactionTableName).ContinueWith(_ => _writerConnection.Close()));

            var hubTransfers =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.CrcHubTransfer) &&
                    o.detail is CrcHubTransfer).ToArray();

            if (hubTransfers.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteHubTransfers(writerConnection, hubTransferTableName, hubTransfers)
                    .ContinueWith(_ => writerConnection.Close()));
            }

            var organisationSignups =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.CrcOrganisationSignup) &&
                    o.detail is CrcOrganisationSignup).ToArray();

            if (organisationSignups.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteOrganisationSignups(writerConnection, organisationSignupTableName,
                    organisationSignups).ContinueWith(_ => writerConnection.Close()));
            }

            var signups =
                details.Where(o =>
                        o.transaction.Classification.HasFlag(TransactionClass.CrcSignup) && o.detail is CrcSignup)
                    .ToArray();

            if (signups.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteSignups(writerConnection, signupTableName, signups)
                    .ContinueWith(_ => writerConnection.Close()));
            }

            var trusts =
                details.Where(o =>
                        o.transaction.Classification.HasFlag(TransactionClass.CrcTrust) && o.detail is CrcTrust)
                    .ToArray();

            if (trusts.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteTrusts(writerConnection, trustTableName, trusts)
                    .ContinueWith(_ => writerConnection.Close()));
            }

            var erc20Transfers =
                details.Where(o =>
                        o.transaction.Classification.HasFlag(TransactionClass.Erc20Transfer) &&
                        o.detail is Erc20Transfer)
                    .ToArray();

            if (erc20Transfers.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteErc20Transfers(writerConnection, erc20TransferTableName, erc20Transfers)
                    .ContinueWith(_ => writerConnection.Close()));
            }

            var ethTransfers =
                details.Where(o =>
                        o.transaction.Classification.HasFlag(TransactionClass.EoaEthTransfer) &&
                        o.detail is EthTransfer)
                    .ToArray();

            if (ethTransfers.Length > 0)
            {
                var writerConnection = await GetDbConnection(writerConnectionString);
                promises.Add(StagingTables.WriteEthTransfers(writerConnection, ethTransferTableName, ethTransfers)
                    .ContinueWith(_ => writerConnection.Close()));
            }

            var safeEthTransfers =
                details.Where(o =>
                        o.transaction.Classification.HasFlag(TransactionClass.SafeEthTransfer) &&
                        o.detail is GnosisSafeEthTransfer)
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
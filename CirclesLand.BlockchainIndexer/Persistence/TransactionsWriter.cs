using System;
using System.Collections.Generic;
using System.Linq;
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
        
        public static void WriteTransactions(
            NpgsqlConnection writerConnection,
            IEnumerable<(
                int TotalTransactionsInBlock,
                string TxHash,
                HexBigInteger Timestamp,
                Transaction Transaction,
                TransactionReceipt Receipt,
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

            BlockWriter.WriteBlocks(writerConnection, blockTableName, blockList);

            var details =
                transactionsWithExtractedDetailsArr.SelectMany(transaction =>
                        transaction.Details.Select(detail => (transaction, detail)))
                    .ToArray();

            StagingTables.WriteTransactionRows(
                writerConnection,
                transactionsWithExtractedDetailsArr,
                transactionTableName);

            var hubTransfers =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.CrcHubTransfer) &&
                    o.detail is CrcHubTransfer);

            StagingTables.WriteHubTransfers(writerConnection, hubTransferTableName, hubTransfers);

            var organisationSignups =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.CrcOrganisationSignup) &&
                    o.detail is CrcOrganisationSignup);

            StagingTables.WriteOrganisationSignups(writerConnection, organisationSignupTableName, organisationSignups);

            var signups =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.CrcSignup) && o.detail is CrcSignup);

            StagingTables.WriteSignups(writerConnection, signupTableName, signups);

            var trusts =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.CrcTrust) && o.detail is CrcTrust);

            StagingTables.WriteTrusts(writerConnection, trustTableName, trusts);

            var erc20Transfers =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.Erc20Transfer) && o.detail is Erc20Transfer);

            StagingTables.WriteErc20Transfers(writerConnection, erc20TransferTableName, erc20Transfers);

            var ethTransfers =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.EoaEthTransfer) && o.detail is EthTransfer);

            StagingTables.WriteEthTransfers(writerConnection, ethTransferTableName, ethTransfers);

            var safeEthTransfers =
                details.Where(o =>
                    o.transaction.Classification.HasFlag(TransactionClass.SafeEthTransfer) &&
                    o.detail is GnosisSafeEthTransfer);

            StagingTables.WriteSafeEthTransfers(writerConnection, gnosisSafeEthTransferTableName, safeEthTransfers);
        }
    }
}
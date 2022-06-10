using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;
using Npgsql;
using NpgsqlTypes;

namespace CirclesLand.BlockchainIndexer.Persistence
{
    public class StagingTables
    {
        public static void Truncate(NpgsqlConnection connection)
        {
            connection.Execute(@"
                            truncate _block_staging;
                            truncate _crc_hub_transfer_staging;
                            truncate _crc_organisation_signup_staging;
                            truncate _crc_signup_staging;
                            truncate _erc20_transfer_staging;
                            truncate _eth_transfer_staging;
                            truncate _gnosis_safe_eth_transfer_staging;
                            truncate _transaction_staging;
                        ");
        }

        /// <summary>
        /// Cleans all imported entries from the staging table and returns all hashes of the imported transactions.
        /// </summary>
        public static string[] CleanImported(NpgsqlConnection connection)
        {
            var cleanupSql = @"
                delete from _gnosis_safe_eth_transfer_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                delete from _eth_transfer_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                delete from _erc20_transfer_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                delete from _crc_trust_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                delete from _crc_signup_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                delete from _crc_organisation_signup_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                delete from _crc_hub_transfer_staging where block_number in (select distinct number from _block_staging where imported_at is not null);
                delete from _transaction_staging where block_number in (select distinct number from _block_staging where imported_at is not null) returning hash;
                delete from _block_staging where imported_at is not null;";

            var t = connection.BeginTransaction(IsolationLevel.ReadCommitted);
            
            var newTransactions = connection.Query(cleanupSql, null, t, true, 20);
            t.Commit();

            return newTransactions.Select(o => (string)o.hash).ToArray();
        }
        public static async Task<int> WriteSafeEthTransfers(NpgsqlConnection writerConnection,
            string? safeEthTransferTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt? Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>
                safeEthTransfers)
        {
            await using var writer = await writerConnection.BeginBinaryImportAsync(
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

            var rowCount = 0;
            foreach (var d in safeEthTransfers)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                await writer.StartRowAsync();

                await writer.WriteAsync(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                await writer.WriteAsync((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                await writer.WriteAsync(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                await writer.WriteAsync((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                await writer.WriteAsync(((GnosisSafeEthTransfer) d.detail).Initiator, NpgsqlDbType.Text);
                await writer.WriteAsync(((GnosisSafeEthTransfer) d.detail).From, NpgsqlDbType.Text);
                await writer.WriteAsync(((GnosisSafeEthTransfer) d.detail).To, NpgsqlDbType.Text);
                await writer.WriteAsync(((GnosisSafeEthTransfer) d.detail).Value, NpgsqlDbType.Text);

                rowCount++;
            }

            await writer.CompleteAsync();

            return rowCount;
        }

        public static async Task<int> WriteEthTransfers(NpgsqlConnection writerConnection,
            string ethTransferTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt? Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>
                ethTransfers)
        {
            await using var writer = await writerConnection.BeginBinaryImportAsync(
                @$"COPY {ethTransferTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,""from""
                                        ,""to""
                                        ,value
                                    ) FROM STDIN (FORMAT BINARY)");

            var rowCount = 0;
            foreach (var d in ethTransfers)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                await writer.StartRowAsync();

                await writer.WriteAsync(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                await writer.WriteAsync((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                await writer.WriteAsync(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                await writer.WriteAsync((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                await writer.WriteAsync(((EthTransfer) d.detail).From, NpgsqlDbType.Text);
                await writer.WriteAsync(((EthTransfer) d.detail).To, NpgsqlDbType.Text);
                await writer.WriteAsync(((EthTransfer) d.detail).Value, NpgsqlDbType.Text);

                rowCount++;
            }

            await writer.CompleteAsync();
            return rowCount;
        }

        public static async Task<int> WriteTrusts(NpgsqlConnection writerConnection,
            string trustTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt? Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>
                trusts)
        {
            await using var writer = await writerConnection.BeginBinaryImportAsync(
                @$"COPY {trustTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,address
                                        ,can_send_to
                                        ,""limit""
                                    ) FROM STDIN (FORMAT BINARY)");

            var rowCount = 0;
            foreach (var d in trusts)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                await writer.StartRowAsync();

                await writer.WriteAsync(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                await writer.WriteAsync((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                await writer.WriteAsync(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                await writer.WriteAsync((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                await writer.WriteAsync(((CrcTrust) d.detail).Address, NpgsqlDbType.Text);
                await writer.WriteAsync(((CrcTrust) d.detail).CanSendTo, NpgsqlDbType.Text);
                await writer.WriteAsync((long) ((CrcTrust) d.detail).Limit, NpgsqlDbType.Numeric);

                rowCount++;
            }

            await writer.CompleteAsync();
            
            return rowCount;
        }

        public static async Task<int> WriteSignups(NpgsqlConnection writerConnection,
            string signupsTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt? Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>
                signups)
        {
            await using var writer = await writerConnection.BeginBinaryImportAsync(
                @$"COPY {signupsTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,""user""
                                        ,token
                                        ,owners
                                    ) FROM STDIN (FORMAT BINARY)");

            var rowCount = 0;
            foreach (var d in signups)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                await writer.StartRowAsync();

                await writer.WriteAsync(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                await writer.WriteAsync((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                await writer.WriteAsync(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                await writer.WriteAsync((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                await writer.WriteAsync(((CrcSignup) d.detail).User, NpgsqlDbType.Text);
                await writer.WriteAsync(((CrcSignup) d.detail).Token, NpgsqlDbType.Text);
                await writer.WriteAsync(((CrcSignup) d.detail).Owners, NpgsqlDbType.Array | NpgsqlDbType.Text);

                rowCount++;
            }

            await writer.CompleteAsync();
            
            return rowCount;
        }

        public static async Task<int> WriteOrganisationSignups(NpgsqlConnection writerConnection,
            string organisationSignupsTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt? Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>
                organisationSignups)
        {
            await using var writer = await writerConnection.BeginBinaryImportAsync(
                @$"COPY {organisationSignupsTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,organisation
                                        ,owners
                                    ) FROM STDIN (FORMAT BINARY)");

            var rowCount = 0;
            foreach (var d in organisationSignups)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                await writer.StartRowAsync();

                await writer.WriteAsync(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                await writer.WriteAsync((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                await writer.WriteAsync(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                await writer.WriteAsync((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                await writer.WriteAsync(((CrcOrganisationSignup) d.detail).Organization, NpgsqlDbType.Text);
                await writer.WriteAsync(((CrcOrganisationSignup) d.detail).Owners, NpgsqlDbType.Array | NpgsqlDbType.Text);

                rowCount++;
            }

            await writer.CompleteAsync();

            return rowCount;
        }

        public static async Task<int> WriteHubTransfers(NpgsqlConnection writerConnection,
            string hubTransfersTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt? Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>
                hubTransfers)
        {
            await using var writer = await writerConnection.BeginBinaryImportAsync(
                @$"COPY {hubTransfersTableName} (
                                         hash
                                        ,index
                                        ,timestamp
                                        ,block_number
                                        ,""from""
                                        ,""to""
                                        ,value
                                    ) FROM STDIN (FORMAT BINARY)");

            var rowCount = 0;
            foreach (var d in hubTransfers)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                await writer.StartRowAsync();

                await writer.WriteAsync(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                await writer.WriteAsync((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                await writer.WriteAsync(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                await writer.WriteAsync((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                await writer.WriteAsync(((CrcHubTransfer) d.detail).From, NpgsqlDbType.Text);
                await writer.WriteAsync(((CrcHubTransfer) d.detail).To, NpgsqlDbType.Text);
                await writer.WriteAsync(((CrcHubTransfer) d.detail).Value, NpgsqlDbType.Text);

                rowCount++;
            }

            await writer.CompleteAsync();

            return rowCount;
        }

        public static async Task<int> WriteErc20Transfers(NpgsqlConnection writerConnection, string? erc20TransferTableName,
            IEnumerable<((int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt? Receipt, TransactionClass Classification, IDetail[] Details) transaction, IDetail
                    detail
                    )>
                erc20Transfers)
        {
            await using var writer = await writerConnection.BeginBinaryImportAsync(
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

            var rowCount = 0;
            foreach (var d in erc20Transfers)
            {
                var blockTimestamp = d.transaction.Timestamp.ToLong();
                var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

                await writer.StartRowAsync();

                await writer.WriteAsync(d.transaction.Transaction.TransactionHash, NpgsqlDbType.Text);
                await writer.WriteAsync((int) d.transaction.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                await writer.WriteAsync(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                await writer.WriteAsync((long) d.transaction.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                await writer.WriteAsync(((Erc20Transfer) d.detail).From, NpgsqlDbType.Text);
                await writer.WriteAsync(((Erc20Transfer) d.detail).To, NpgsqlDbType.Text);
                await writer.WriteAsync(((Erc20Transfer) d.detail).Token, NpgsqlDbType.Text);
                await writer.WriteAsync(((Erc20Transfer) d.detail).Value, NpgsqlDbType.Text);

                rowCount++;
            }

            await writer.CompleteAsync();

            return rowCount;
        }

        public static async Task<int> WriteTransactionRows(NpgsqlConnection writerConnection,
            IEnumerable<(int TotalTransactionsInBlock, string TxHash, HexBigInteger Timestamp, Transaction Transaction,
                    TransactionReceipt? Receipt, TransactionClass Classification, IDetail[] Details)>
                transactionsWithExtractedDetails,
            string transactionTableName)
        {
            await using var writer = await writerConnection.BeginBinaryImportAsync(
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

            var rowCount = 0;
            foreach (var t in transactionsWithExtractedDetails)
            {
                var blockTimestamp = t.Timestamp.ToLong();
                var blockTimestampDateTime =
                    DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;
                var classificationArray = t.Classification.ToString()
                    .Split(",", StringSplitOptions.TrimEntries);

                await writer.StartRowAsync();

                await writer.WriteAsync((long) t.Transaction.BlockNumber.Value, NpgsqlDbType.Bigint);
                await writer.WriteAsync(t.Transaction.From, NpgsqlDbType.Text);
                await writer.WriteAsync(t.Transaction.To, NpgsqlDbType.Text);
                await writer.WriteAsync(t.Transaction.TransactionHash, NpgsqlDbType.Text);
                await writer.WriteAsync((int) t.Transaction.TransactionIndex.Value, NpgsqlDbType.Integer);
                await writer.WriteAsync(blockTimestampDateTime, NpgsqlDbType.Timestamp);
                await writer.WriteAsync(t.Transaction.Value.Value.ToString(), NpgsqlDbType.Text);
                await writer.WriteAsync(t.Transaction.Input, NpgsqlDbType.Text);
                await writer.WriteAsync(t.Transaction.Nonce.ToString(), NpgsqlDbType.Text);
                await writer.WriteAsync(t.Transaction.Type?.ToString() ?? "0", NpgsqlDbType.Text);
                await writer.WriteAsync(classificationArray, NpgsqlDbType.Array | NpgsqlDbType.Text);

                rowCount++;
            }

            await writer.CompleteAsync();

            return rowCount;
        }
    }
}
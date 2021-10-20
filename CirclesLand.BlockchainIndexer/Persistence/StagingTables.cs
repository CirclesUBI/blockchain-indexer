using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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
        public static int WriteSafeEthTransfers(NpgsqlConnection writerConnection,
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

            var rowCount = 0;
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

                rowCount++;
            }

            writer.Complete();

            return rowCount;
        }

        public static int WriteEthTransfers(NpgsqlConnection writerConnection,
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

            var rowCount = 0;
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

                rowCount++;
            }

            writer.Complete();
            return rowCount;
        }

        public static int WriteTrusts(NpgsqlConnection writerConnection,
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

            var rowCount = 0;
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

                rowCount++;
            }

            writer.Complete();
            
            return rowCount;
        }

        public static int WriteSignups(NpgsqlConnection writerConnection,
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
                                        ,owners
                                    ) FROM STDIN (FORMAT BINARY)");

            var rowCount = 0;
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
                writer.Write(((CrcSignup) d.detail).Owners, NpgsqlDbType.Array | NpgsqlDbType.Text);

                rowCount++;
            }

            writer.Complete();
            
            return rowCount;
        }

        public static int WriteOrganisationSignups(NpgsqlConnection writerConnection,
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
                                        ,owners
                                    ) FROM STDIN (FORMAT BINARY)");

            var rowCount = 0;
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
                writer.Write(((CrcOrganisationSignup) d.detail).Owners, NpgsqlDbType.Array | NpgsqlDbType.Text);

                rowCount++;
            }

            writer.Complete();

            return rowCount;
        }

        public static int WriteHubTransfers(NpgsqlConnection writerConnection,
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

            var rowCount = 0;
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

                rowCount++;
            }

            writer.Complete();

            return rowCount;
        }

        public static int WriteErc20Transfers(NpgsqlConnection writerConnection, string? erc20TransferTableName,
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

            var rowCount = 0;
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

                rowCount++;
            }

            writer.Complete();

            return rowCount;
        }

        public static int WriteTransactionRows(NpgsqlConnection writerConnection,
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

            var rowCount = 0;
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

                rowCount++;
            }

            writer.Complete();

            return rowCount;
        }
    }
}
using System;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.RPC.Eth.DTOs;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence
{
    public class TransactionWriter
    {
        private const string InsertTransactionLogSql = @"
            insert into TransactionLog (
                TransactionId
                , LogIndex
                , Address
                , Data
            ) values (
                @1, @2, @3, @4
            )
            returning Id;
        ";

        private const string InsertTransactionLogTopicSql = @"
            insert into TransactionLogTopic (
                TransactionLogId
                , Topic
            ) values (
                    @1, @2
            )
            returning Id;
        ";

        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlTransaction? _dbTransaction;

        public TransactionWriter(NpgsqlConnection connection, NpgsqlTransaction? dbTransaction)
        {
            _connection = connection;
            _dbTransaction = dbTransaction;
        }

        public void Write(
            int totalTransactionCount, 
            DateTime blockTimestamp, 
            TransactionClass transactionClass, 
            Transaction transaction)
        {
            _connection.Execute(
                @"insert into block (number, hash, timestamp, total_transaction_count, indexed_transaction_count) 
                         values (@number, @hash, @timestamp, @total_transaction_count, @indexed_transaction_count)
                         on conflict do nothing;", new
                {
                    number = transaction.BlockNumber.ToLong(),
                    hash = transaction.BlockHash,
                    timestamp = blockTimestamp,
                    total_transaction_count = totalTransactionCount,
                    indexed_transaction_count = 0
                }, _dbTransaction);

            var classificationArray = transactionClass.ToString()
                .Split(",", StringSplitOptions.TrimEntries);

            const string InsertTransactionSql = @"
                insert into transaction_2 (
                      block_number
                    , ""from""
                    , ""to""
                    , hash
                    , index
                    , timestamp
                    , value
                    , nonce
                    , type
                    , input
                    , classification
                ) values (
                    @block_number, 
                    @from, 
                    @to, 
                    @hash,
                    @index, 
                    @timestamp,
                    @value::numeric, 
                    @nonce, 
                    @type, 
                    @input, 
                    @classification
                )
                on conflict do nothing;";

            _connection.Execute(InsertTransactionSql, new
            {
                block_number = transaction.BlockNumber.ToLong(),
                from = transaction.From?.ToLowerInvariant(),
                to = transaction.To?.ToLowerInvariant(),
                index = (int) transaction.TransactionIndex.ToLong(),
                hash = transaction.TransactionHash,
                timestamp = blockTimestamp,
                value = transaction.Value.ToString(),
                nonce = transaction.Nonce.ToString(),
                type = transaction.Type.ToString(),
                input = transaction.Input,
                classification = classificationArray
            });
        }
    }
}
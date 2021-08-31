using System;
using System.Collections.Generic;
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
        
        public long? Write(bool blockOnly, int totalTransactionCount, DateTime blockTimestamp, TransactionClass transactionClass, Transaction transaction, IEnumerable<IDetail> details)
        {
            var existingBlock = _connection.QuerySingleOrDefault<long>(
                "select number from block where number = @number;",
                new
                {
                    number = transaction.BlockNumber.ToLong()
                }, _dbTransaction);

            if (existingBlock == 0)
            {
                _connection.Execute(
                    @"insert into block (number, hash, timestamp, total_transaction_count, indexed_transaction_count) 
                         values (@number, @hash, @timestamp, @total_transaction_count, @indexed_transaction_count);", new
                    {
                        number = transaction.BlockNumber.ToLong(),
                        hash = transaction.BlockHash,
                        timestamp = blockTimestamp,
                        total_transaction_count = totalTransactionCount,
                        indexed_transaction_count = 0
                    }, _dbTransaction);
            }

            if (blockOnly)
            {
                _connection.Execute(
                    @"update block set 
                        indexed_transaction_count = indexed_transaction_count + 1 
                     where number = @number;", new
                    {
                        number = transaction.BlockNumber.ToLong()
                    }, _dbTransaction);
                
                return null;
            } 
            
            var existingTransactionId = _connection.QuerySingleOrDefault<long>(
                "select \"id\" from \"transaction\" where \"hash\" = @transaction_hash;",new
                {
                    transaction_hash = transaction.TransactionHash
                }, _dbTransaction);

            if (existingTransactionId != 0)
            {
                return existingTransactionId;
            }

            var classificationArray = transactionClass.ToString()
                .Split(",", StringSplitOptions.TrimEntries);
            
            const string InsertTransactionSql = @"
                insert into transaction (
                      block_number
                    , ""from""
                    , ""to""
                    , index
                    , gas
                    , hash
                    , value
                    , nonce
                    , type
                    , input
                    , classification
                ) values (
                    @block_number, 
                    @from, 
                    @to, 
                    @index, 
                    @gas::numeric, 
                    @hash,
                    @value::numeric, 
                    @nonce, 
                    @type, 
                    @input, 
                    @classification
                )
                returning Id;
            ";

            var transactionId = _connection.QuerySingle<long>(InsertTransactionSql, new
            {
                block_number = transaction.BlockNumber.ToLong(),
                from = transaction.From,
                to = transaction.To,
                index = (int)transaction.TransactionIndex.ToLong(),
                gas = transaction.Gas.ToString(),
                hash = transaction.TransactionHash,
                value = transaction.Value.ToString(),
                nonce = transaction.Nonce.ToString(),
                type = transaction.Type.ToString(),
                input = transaction.Input,
                classification = classificationArray
            });
            
            _connection.Execute(
                @"update block set 
                        indexed_transaction_count = indexed_transaction_count + 1 
                     where number = @number;", new
                {
                    number = transaction.BlockNumber.ToLong()
                }, _dbTransaction);
            
            return transactionId;
        }
    }
}
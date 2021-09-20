using System;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class EthTransferWriter
    {
        public static void Insert (
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            string hash,
            int index,
            DateTime timestamp,
            long block_number,
            EthTransfer data)
        {
            const string InsertEthTransferSql = @"
            insert into eth_transfer_2 (
                      hash
                    , index                    
                    , timestamp                
                    , block_number
                    , ""from""
                    , ""to""
                    , value
                ) values (
                    @hash, @index, @timestamp, @block_number, @from, @to, @value::numeric
                )
                on conflict do nothing;
            ";
            
            connection.Execute(InsertEthTransferSql, new
            {
                hash,
                index,
                timestamp,
                block_number,
                from = data.From?.ToLowerInvariant(),
                to = data.To?.ToLowerInvariant(),
                value = data.Value
            }, dbTransaction);
        }
    }
}
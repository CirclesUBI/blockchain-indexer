using System;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class CrcTransferWriter
    {
        public static void Insert(
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            string hash,
            int index,
            DateTime timestamp,
            long block_number,
            CrcHubTransfer data)
        {
            const string InsertCrcTransferSql = @"
                insert into crc_hub_transfer_staging (
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
            
            connection.Execute(InsertCrcTransferSql, new
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
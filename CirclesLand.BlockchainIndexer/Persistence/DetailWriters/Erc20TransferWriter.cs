using System;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class Erc20TransferWriter
    {
        public static void Insert (
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            string hash,
            int index,
            DateTime timestamp,
            long block_number,
            Erc20Transfer data)
        {
            const string InserErc20TransferSql = @"
                insert into erc20_transfer_staging (
                      hash
                    , index                    
                    , timestamp                
                    , block_number
                    , ""from""
                    , ""to""
                    , token
                    , value
                ) values (
                    @hash, @index, @timestamp, @block_number, @from, @to, @token, @value::numeric
                )
                on conflict do nothing;
            ";
            
            connection.Execute(InserErc20TransferSql, new
            {
                hash,
                index,
                timestamp,
                block_number,
                from = data.From?.ToLowerInvariant(),
                to = data.To?.ToLowerInvariant(),
                token = data.Token?.ToLowerInvariant(),
                value = data.Value
            }, dbTransaction);
        }
    }
}
using System;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class GnosisSafeEthTransferWriter
    {
        public static void Insert (
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            string hash,
            int index,
            DateTime timestamp,
            long block_number,
            GnosisSafeEthTransfer data)
        {
            const string InsertGnosisSafeEthTransferSql = @"
                insert into gnosis_safe_eth_transfer_staging (
                      hash
                    , index                    
                    , timestamp                
                    , block_number
                    , initiator
                    , ""from""
                    , ""to""
                    , value
                ) values (
                    @hash, @index, @timestamp, @block_number, @initiator, @from, @to, @value::numeric
                )
                on conflict do nothing;
            ";
            
            connection.Execute(InsertGnosisSafeEthTransferSql, new
            {
                hash,
                index,
                timestamp,
                block_number,
                initiator = data.Initiator?.ToLowerInvariant(),
                from = data.From?.ToLowerInvariant(),
                to = data.To?.ToLowerInvariant(),
                value = data.Value
            }, dbTransaction);
        }
    }
}
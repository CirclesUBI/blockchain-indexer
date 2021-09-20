using System;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class CrcTrustWriter
    {
        public static void Insert(
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            string hash,
            int index,
            DateTime timestamp,
            long block_number,
            CrcTrust data)
        {
            const string InsertCrcTrustSql = @"
                insert into crc_trust_2 (
                      hash
                    , index                    
                    , timestamp                
                    , block_number
                    , address
                    , can_send_to
                    , ""limit""
                ) values (
                    @hash, @index, @timestamp, @block_number, @address, @can_send_to, @limit::numeric      
                )
                on conflict do nothing;
            ";
            
            connection.Execute(InsertCrcTrustSql, new
            {
                hash,
                index,
                timestamp,
                block_number,
                address = data.Address?.ToLowerInvariant(),
                can_send_to = data.CanSendTo?.ToLowerInvariant(),
                limit = data.Limit
            }, dbTransaction);
        }
    }
}
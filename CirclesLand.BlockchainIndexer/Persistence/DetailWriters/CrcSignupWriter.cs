using System;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class CrcSignupWriter
    {
        public static void Insert(
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            string hash,
            int index,
            DateTime timestamp,
            long block_number,
            CrcSignup data)
        {
            const string InsertCrcSignupSql = @"
                insert into crc_signup_2 (
                      hash
                    , index                    
                    , timestamp                
                    , block_number
                    , ""user""
                    , token
                ) values (
                    @hash, @index, @timestamp, @block_number, @user, @token
                )
                on conflict do nothing;
            ";
            
            connection.Execute(InsertCrcSignupSql, new
            {
                hash,
                index,
                timestamp,
                block_number,
                user = data.User?.ToLowerInvariant(),
                token = data.Token?.ToLowerInvariant()
            }, dbTransaction);
        }
    }
}
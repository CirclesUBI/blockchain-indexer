using System;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class CrcOrganisationSignupWriter
    {
        public static void Insert(
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            string hash,
            int index,
            DateTime timestamp,
            long block_number,
            CrcOrganisationSignup data)
        {
            const string InsertCrcOrganisationSql = @"
                insert into crc_organisation_signup_staging (
                      hash
                    , index                    
                    , timestamp                
                    , block_number
                    , organisation
                ) values (
                    @hash, @index, @timestamp, @block_number, @organisation
                )
                on conflict do nothing;
            ";
            
            connection.Execute(InsertCrcOrganisationSql, new
            {
                hash,
                index,
                timestamp,
                block_number,
                organisation = data.Organization?.ToLowerInvariant()
            }, dbTransaction);
        }
    }
}
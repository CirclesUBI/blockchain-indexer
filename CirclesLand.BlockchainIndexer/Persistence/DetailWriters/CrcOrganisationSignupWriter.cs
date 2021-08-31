using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class CrcOrganisationSignupWriter
    {
        public static long Insert(
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            long transactionId, 
            CrcOrganisationSignup data)
        {
            const string InsertCrcOrganisationSql = @"
                insert into crc_organisation_signup (
                      transaction_id
                    , organisation
                ) values (
                    @transaction_id, @organisation
                )
                returning id;
            ";
            
            return connection.QuerySingle<long>(InsertCrcOrganisationSql, new
            {
                transaction_id = transactionId,
                organisation = data.Organization?.ToLowerInvariant()
            }, dbTransaction);
        }
    }
}
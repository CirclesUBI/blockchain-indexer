using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class CrcSignupWriter
    {
        public static long Insert(
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            long transactionId, 
            CrcSignup data)
        {
            const string InsertCrcSignupSql = @"
                insert into crc_signup (
                      transaction_id
                    , user
                    , token
                ) values (
                    @transaction_id, @user, @token
                )
                returning id;
            ";
            
            return connection.QuerySingle<long>(InsertCrcSignupSql, new
            {
                transaction_id = transactionId,
                user = data.User,
                token = data.Token
            }, dbTransaction);
        }
    }
}
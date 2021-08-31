using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class CrcTrustWriter
    {
        public static long Insert(
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            long transactionId, 
            CrcTrust data)
        {
            const string InsertCrcTrustSql = @"
                insert into crc_trust (
                      transaction_id
                    , address
                    , can_send_to
                    , ""limit""
                ) values (
                    @transaction_id, @address, @can_send_to, @limit::numeric      
                )
                returning id;
            ";
            
            return connection.QuerySingle<long>(InsertCrcTrustSql, new
            {
                transaction_id = transactionId,
                address = data.Address,
                can_send_to = data.CanSendTo,
                limit = data.Limit
            }, dbTransaction);
        }
    }
}
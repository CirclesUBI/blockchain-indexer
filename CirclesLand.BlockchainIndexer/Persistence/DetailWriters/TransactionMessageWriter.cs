using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class TransactionMessageWriter
    {
        public static long Insert(
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            long transactionId, 
            TransactionMessage data)
        {
            const string InsertTransactionMessageSql = @"
                insert into transaction_message (
                    transaction_id
                    , text
                ) values (
                    @transaction_id, @text      
                )
                returning id;
            ";
            
            return connection.QuerySingle<long>(InsertTransactionMessageSql, new
            {
                transaction_id = transactionId,
                text = data.Text
            }, dbTransaction);
        }
    }
}
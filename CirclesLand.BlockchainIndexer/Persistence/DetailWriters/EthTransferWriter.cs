using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class EthTransferWriter
    {
        public static long Insert (
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            long transactionId, 
            EthTransfer data)
        {
            const string InsertEthTransferSql = @"
            insert into eth_transfer (
                      transaction_id
                    , ""from""
                    , ""to""
                    , value
                ) values (
                    @transaction_id, @from, @to, @value::numeric
                )
                returning id;
            ";
            
            return connection.QuerySingle<long>(InsertEthTransferSql, new
            {
                transaction_id = transactionId,
                from = data.From,
                to = data.To,
                value = data.Value
            }, dbTransaction);
        }
    }
}
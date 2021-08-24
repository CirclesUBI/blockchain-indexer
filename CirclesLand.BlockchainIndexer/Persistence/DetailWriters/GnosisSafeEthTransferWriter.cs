using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class GnosisSafeEthTransferWriter
    {
        public static long Insert (
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            long transactionId, 
            EthTransfer data)
        {
            const string InsertGnosisSafeEthTransferSql = @"
                insert into gnosis_safe_eth_transfer (
                    transaction_id
                    , ""from""
                    , ""to""
                    , value
                ) values (
                    @transaction_id, @from, @to, @value
                )
                returning id;
            ";
            
            return connection.QuerySingle<long>(InsertGnosisSafeEthTransferSql, new
            {
                transaction_id = transactionId,
                from = data.From,
                to = data.To,
                value = data.Value
            }, dbTransaction);
        }
    }
}
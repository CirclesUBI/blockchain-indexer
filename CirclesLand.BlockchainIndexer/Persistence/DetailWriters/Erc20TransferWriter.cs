using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class Erc20TransferWriter
    {
        public static long Insert (
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            long transactionId, 
            Erc20Transfer data)
        {
            const string InserErc20TransferSql = @"
                insert into erc20_transfer (
                      transaction_id
                    , ""from""
                    , ""to""
                    , token
                    , value
                ) values (
                    @transaction_id, @from, @to, @token, @value
                )
                returning id;
            ";
            
            return connection.QuerySingle<long>(InserErc20TransferSql, new
            {
                transaction_id = transactionId,
                from = data.From,
                to = data.To,
                token = data.Token,
                value = data.Value
            }, dbTransaction);
        }
    }
}
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class TokenMintingWriter
    {
        public static long Insert (
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            long transactionId, 
            TokenMinting data)
        {
            const string InsertTokenMintingSql = @"
                insert into token_minting (
                    transaction_id
                    , ""to""
                    , token
                    , tokens
                ) values (
                    @transaction_id, @to, @token, @tokens
                ) 
                returning id;
            ";
            
            return connection.QuerySingle<long>(InsertTokenMintingSql, new
            {
                transaction_id = transactionId,
                to = data.To,
                token = data.Token,
                tokens = data.Tokens
            }, dbTransaction);
        }
    }
}
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class CrcTransferWriter
    {
        public static long Insert(
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            long transactionId, 
            CrcHubTransfer data)
        {
            const string InsertCrcTransferSql = @"
                insert into crc_hub_transfer (
                    transaction_id
                    , ""from""
                    , ""to""
                    , value
                ) values (
                    @transaction_id, @from, @to, @value::numeric
                )
                returning Id;
            ";
            
            return connection.QuerySingle<long>(InsertCrcTransferSql, new
            {
                transaction_id = transactionId,
                from = data.From,
                to = data.To,
                value = data.Value
            }, dbTransaction);
        }
    }
}
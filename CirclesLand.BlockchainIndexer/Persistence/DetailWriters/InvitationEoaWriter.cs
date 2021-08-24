using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class InvitationEoaWriter
    {
        public static long Insert(
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction,
            InvitationEoa data)
        {
            const string InsertTransactionMessageSql = @"
                insert into invitation_eoa (
                      owner_id
                    , name
                    , redeem_code
                    , eoa_address
                    , eoa_key
                ) values (
                    @owner_id, @name, @redeem_code, @eoa_address, @eoa_key      
                )
                returning id;
            ";
            
            return connection.QuerySingle<long>(InsertTransactionMessageSql, new
            {
                owner_id = data.OwnerId,
                name = data.Name,
                redeem_code = data.RedeemCode,
                eoa_address = data.EoaAddress,
                eoa_key = data.EoaKey,
                
            }, dbTransaction);
        }
    }
}
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence.DetailWriters
{
    public static class InvitationRedemptionWriter
    {
        public static long Insert(
            NpgsqlConnection connection, 
            NpgsqlTransaction? dbTransaction, 
            long transactionId, 
            InvitationRedemption data)
        {
            const string InsertInvitationRedemptionSql = @"
                insert into invitation_eoa_redemption (
                      transaction_id
                    , invitation_eoa_id
                    , eth_transfer_id
                ) values (
                    @transaction_id, @invitation_eoa_id, @eth_transfer_id    
                )
                returning id;
            ";
            
            return connection.QuerySingle<long>(InsertInvitationRedemptionSql, new
            {
                transaction_id = transactionId,
                invitation_eoa_id = data.InvitationEoaId,
                eth_transfer_id = data.EthTransferId
            }, dbTransaction);
        }
    }
}
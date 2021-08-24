namespace CirclesLand.BlockchainIndexer.TransactionDetailModels
{
    public class InvitationRedemption : Detail
    {
        public long InvitationEoaId { get; set; }
        public long EthTransferId { get; set; }
    }
}
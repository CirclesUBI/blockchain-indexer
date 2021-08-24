namespace CirclesLand.BlockchainIndexer.TransactionDetailModels
{
    public class InvitationEoa : Detail
    {
        public string? OwnerId { get; set; }
        public string? RedeemCode { get; set; }
        public string? EoaAddress { get; set; }
        public string? EoaKey { get; set; }
        public string? Name { get; set; }
    }
}
namespace CirclesLand.BlockchainIndexer.TransactionDetailModels
{
    public class CrcTrust : Detail
    {
        public string? Address { get; set; }
        public string? CanSendTo { get; set; }
        public long Limit { get; set; }
    }
}
namespace CirclesLand.BlockchainIndexer.TransactionDetailModels
{
    public class EthTransfer : Detail
    {
        public string? From { get; set; }
        public string? To { get; set; }
        public string? Value { get; set; }
    }
}
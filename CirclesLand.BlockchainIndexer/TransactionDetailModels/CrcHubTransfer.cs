namespace CirclesLand.BlockchainIndexer.TransactionDetailModels
{
    public class CrcHubTransfer : Detail
    {
        public string? From { get; set; }
        public string? To { get; set; }
        public string? Value { get; set; }
    }
}
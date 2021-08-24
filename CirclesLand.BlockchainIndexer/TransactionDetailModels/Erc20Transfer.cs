namespace CirclesLand.BlockchainIndexer.TransactionDetailModels
{
    public class Erc20Transfer : Detail
    {
        public string? From { get; set; }
        public string? To { get; set; }
        public string? Value { get; set; }
        public string? Token { get; set; }
    }
}
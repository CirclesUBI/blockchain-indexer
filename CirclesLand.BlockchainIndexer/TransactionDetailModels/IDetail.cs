namespace CirclesLand.BlockchainIndexer.TransactionDetailModels
{
    public interface IDetail
    {
        public string Type { get; }
    }

    public abstract class Detail : IDetail
    {
        public string Type => GetType().Name;
    }
}
namespace CirclesLand.BlockchainIndexer.TransactionDetailModels
{
    public class CrcOrganisationSignup : Detail
    {
        public string? Organization { get; set; }

        public string[] Owners { get; set; }
    }
}
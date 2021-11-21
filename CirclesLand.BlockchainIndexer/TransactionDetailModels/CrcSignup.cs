using System;

namespace CirclesLand.BlockchainIndexer.TransactionDetailModels
{
    public class CrcSignup : Detail
    {
        public string? User { get; set; }
        public string? Token { get; set; }

        public string[] Owners { get; set; } = Array.Empty<string>();
    }
}
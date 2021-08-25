using System.Collections.Generic;
using System.Collections.Immutable;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class TransactionDetailExtractor
    {
        public static ImmutableArray<IDetail> Extract(
            TransactionClass transactionClass, 
            Transaction transactionData,
            TransactionReceipt transactionReceipt)
        {
            var details = new List<IDetail>();
            
            if (transactionClass.HasFlag(TransactionClass.CrcTrust))
            {
                details.AddRange(CrcTrustDetailExtractor.Extract(transactionData, transactionReceipt));
            }
            if (transactionClass.HasFlag(TransactionClass.Erc20Transfer))
            {
                details.AddRange(Erc20TransferDetailExtractor.Extract(transactionData, transactionReceipt));
            }
            if (transactionClass.HasFlag(TransactionClass.CrcSignup))
            {
                details.AddRange(CrcSignupDetailExtractor.Extract(transactionData, transactionReceipt));
            }
            if (transactionClass.HasFlag(TransactionClass.CrcHubTransfer))
            {
                details.AddRange(CrcHubTransferDetailExtractor.Extract(transactionData, transactionReceipt));
            }
            if (transactionClass.HasFlag(TransactionClass.CrcOrganisationSignup))
            {
                details.AddRange(CrcOrganisationSignupDetailExtractor.Extract(transactionData, transactionReceipt));
            }

            return details.ToImmutableArray();
        }
    }
}
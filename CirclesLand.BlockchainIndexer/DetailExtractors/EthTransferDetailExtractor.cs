using System.Collections.Generic;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class EthTransferDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            yield return new EthTransfer
            {
                From = transactionData.From,
                To = transactionData.To,
                Value = transactionData.Value.ToString()
            };
        }
    }
}
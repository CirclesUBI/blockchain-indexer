using System;
using System.Collections.Generic;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class CrcHubTransferDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            var isHubTransfer = TransactionClassifier.IsCrcHubTransfer(
                receipt,
                out var from,
                out var to,
                out var amount);

            if (!isHubTransfer || from == null || to == null || amount == null)
            {
                throw new Exception("The supplied transaction and receipt is not a CrcHubTransfer.");
            }

            yield return new CrcHubTransfer
            {
                From = from,
                To = to,
                Value = amount.ToString()
            };
        }
    }
}
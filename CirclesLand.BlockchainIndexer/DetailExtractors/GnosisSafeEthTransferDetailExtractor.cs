using System;
using System.Collections.Generic;
using System.Diagnostics;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.ABI.FunctionEncoding;
using Nethereum.ABI.Model;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class GnosisSafeEthTransferDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            var isSafeEthTransfer = TransactionClassifier.IsSafeEthTransfer(
                transactionData,
                receipt,
                out var initiator,
                out var from,
                out var to,
                out var value);

            if (!isSafeEthTransfer || value == null)
            {
                throw new Exception("The supplied transaction and receipt is not a Erc20Transfer.");
            }
            
            yield return new GnosisSafeEthTransfer
            {
                Initiator = initiator,
                From = from,
                To = to,
                Value = value.Value.ToString()
            };
        }
    }
}
using System;
using System.Collections.Generic;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class CrcSignupDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            var isCrcSignup = TransactionClassifier.IsCrcSignup(receipt, out var userAddress, out var tokenAddress);
            if (!isCrcSignup)
            {
                throw new Exception("The supplied transaction and receipt is not a CrcSignup.");
            }

            yield return new CrcSignup
            {
                User = userAddress,
                Token = tokenAddress
            };
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class TokenMintingDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            var log = receipt.Logs
                .FirstOrDefault(o => o.SelectToken("topics").Values<string>().Contains(TransactionClassifier.TransferEventTopic)
                && o.SelectToken("topics").Values<string>().Contains(TransactionClassifier.EmptyUInt256));

            if (log == null)
            {
                throw new Exception("The supplied transaction is not a valid ERC20 'minting' transaction because " +
                                    $"it misses a log entry with topic {TransactionClassifier.CrcTrustEventTopic}" +
                                    $", {TransactionClassifier.EmptyUInt256} or both.");
            }

            var token = log.Value<string>("address");

            var to = log.SelectToken("topics").Values<string>().Skip(2).First()
                .Replace(TransactionClassifier.AddressEmptyBytesPrefix, "0x");
            
            var tokens = new HexBigInteger(log.Value<string>("data"));

            yield return new TokenMinting
            {
                To = to,
                Token = token,
                Tokens = tokens.ToString()
            };
        }
    }
}
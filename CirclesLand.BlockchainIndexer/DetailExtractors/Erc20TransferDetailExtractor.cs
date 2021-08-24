using System;
using System.Collections.Generic;
using System.Linq;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class Erc20TransferDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            var erc20Logs = receipt.Logs
                .Where(o => o.SelectToken("topics").Values<string>().Contains(TransactionClassifier.TransferEventTopic))
                .ToArray();

            if (!erc20Logs.Any())
            {
                throw new Exception("The supplied transaction is not a valid CRC 'transfer' " +
                                    "transaction because it misses a log entry with " +
                                    $"topic {TransactionClassifier.TransferEventTopic}.");
            }

            foreach (var erc20Log in erc20Logs)
            {
                var token = erc20Log.Value<string>("address");

                var from = erc20Log.SelectToken("topics").Values<string>().Skip(1).First()
                    .Replace(TransactionClassifier.AddressEmptyBytesPrefix, "0x");
                
                var to = erc20Log.SelectToken("topics").Values<string>().Skip(2).First()
                    .Replace(TransactionClassifier.AddressEmptyBytesPrefix, "0x");
                
                var value = new HexBigInteger(erc20Log.Value<string>("data"));

                yield return new Erc20Transfer
                {
                    From = from,
                    To = to,
                    Token = token,
                    Value = value.ToString()
                };
            };
        }
    }
}
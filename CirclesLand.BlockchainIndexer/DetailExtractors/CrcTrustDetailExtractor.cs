using System;
using System.Collections.Generic;
using System.Linq;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class CrcTrustDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            var log = receipt.Logs
                .FirstOrDefault(o =>
                    o.SelectToken("topics").Values<string>()
                        .Contains(TransactionClassifier.CrcTrustEventTopic));

            if (log == null)
            {
                throw new Exception("The supplied transaction is not a valid CRC 'trust' transaction because " +
                                    $"it misses a log entry with topic {TransactionClassifier.CrcTrustEventTopic}.");
            }

            var address = log.SelectToken("topics").Values<string>().Skip(2).First()
                .Replace(TransactionClassifier.AddressEmptyBytesPrefix, "0x");

            var canSendTo = log.SelectToken("topics").Values<string>().Skip(1).First()
                .Replace(TransactionClassifier.AddressEmptyBytesPrefix, "0x");

            var limit = new HexBigInteger(log.Value<string>("data")).ToLong();

            yield return new CrcTrust
            {
                Address = address,
                CanSendTo = canSendTo,
                Limit = limit
            };
        }
    }
}
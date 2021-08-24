using System;
using System.Collections.Generic;
using System.Linq;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class CrcSignupDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            var logs = receipt.Logs;
            var log = logs
                .FirstOrDefault(o => o.SelectToken("topics").Values<string>().Contains(TransactionClassifier.CrcSignupEventTopic));

            if (log == null)
            {
                throw new Exception("The supplied transaction is not a valid CRC 'signup' transaction because " +
                                    $"it misses a log entry with topic {TransactionClassifier.CrcSignupEventTopic}.");
            }

            var user = log.SelectToken("topics").Values<string>().Skip(1).First()
                .Replace(TransactionClassifier.AddressEmptyBytesPrefix, "0x");

            var token = log.Value<string>("data")
                .Replace(TransactionClassifier.AddressEmptyBytesPrefix, "0x");

            yield return new CrcSignup
            {
                User = user,
                Token = token
            };
        }
    }
}
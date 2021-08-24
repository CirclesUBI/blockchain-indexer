using System;
using System.Collections.Generic;
using System.Linq;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class CrcOrganisationSignupDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            var logs = receipt.Logs;
            var log = logs
                .FirstOrDefault(o => o.SelectToken("topics").Values<string>().Contains(TransactionClassifier.CrcOrganisationSignupEventTopic));

            if (log == null)
            {
                throw new Exception("The supplied transaction is not a valid CRC 'organization signup' " +
                                    "transaction because it misses a log entry with " +
                                    $"topic {TransactionClassifier.CrcOrganisationSignupEventTopic}.");
            }

            var organization = log.SelectToken("topics").Values<string>().Skip(1).First()
                .Replace(TransactionClassifier.AddressEmptyBytesPrefix, "0x");

            yield return new CrcOrganisationSignup
            {
                Organization = organization
            };
        }
    }
}
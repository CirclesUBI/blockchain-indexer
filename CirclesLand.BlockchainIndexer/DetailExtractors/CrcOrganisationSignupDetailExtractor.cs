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
            var log = receipt.Logs
                .FirstOrDefault(o => TransactionClassifier.GetTopics(o).Contains(SettingsValues.CrcOrganisationSignupEventTopic));

            if (log == null)
            {
                throw new Exception("The supplied transaction is not a valid CRC 'organization signup' " +
                                    "transaction because it misses a log entry with " +
                                    $"topic {SettingsValues.CrcOrganisationSignupEventTopic}.");
            }

            var isCrcOrganisationSignup =
                TransactionClassifier.IsCrcOrganisationSignup(log, out var organisationAddress);

            if (!isCrcOrganisationSignup)
            {
                throw new Exception("The supplied transaction and receipt is not a CrcOrganisationSignup.");
            }

            yield return new CrcOrganisationSignup
            {
                Organization = organisationAddress
            };
        }
    }
}
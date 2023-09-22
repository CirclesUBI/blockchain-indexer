using System;
using System.Collections.Generic;
using System.Linq;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class CrcTrustDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            var log = receipt.Logs
                .FirstOrDefault(o =>
                    TransactionClassifier.GetTopics(o).Contains(SettingsValues.CrcTrustEventTopic));

            if (log == null)
            {
                throw new Exception("The supplied transaction is not a valid CRC 'trust' transaction because " +
                                    $"it misses a log entry with topic {SettingsValues.CrcTrustEventTopic}.");
            }

            var isCrcTrust = TransactionClassifier.IsCrcTrust(
                log,
                out var canSendTo,
                out var user,
                out var limit);

            if (!isCrcTrust || canSendTo == null || user == null || limit == null)
            {
                throw new Exception("The supplied transaction and receipt is not a CrcTrust.");
            }

            yield return new CrcTrust
            {
                Address = user,
                CanSendTo = canSendTo,
                Limit = limit.ToLong()
            };
        }
    }
}
using System.Collections.Generic;
using System.Linq;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class CrcHubTransferDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            var logs = receipt.Logs;
            var hubTransferEvent = logs
                .Where(o => o.SelectToken("topics").Values<string>().Contains(TransactionClassifier.CrcHubTransferEventTopic))
                .ToArray();

            return hubTransferEvent.Select(o =>
            {
                var topics = o.SelectToken("topics").Values<string>().ToArray();
                var from = topics[1]
                    .Replace(TransactionClassifier.AddressEmptyBytesPrefix, "0x");

                var to = topics[2]
                    .Replace(TransactionClassifier.AddressEmptyBytesPrefix, "0x");

                var value = new HexBigInteger(o.Value<string>("data"));

                return new CrcHubTransfer
                {
                    From = from,
                    To = to,
                    Value = value.ToString()
                };
            });
        }
    }
}
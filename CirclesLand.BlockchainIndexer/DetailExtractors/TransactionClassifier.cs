using System.Linq;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public class TransactionClassifier
    {
        public const string AddressEmptyBytesPrefix = "0x000000000000000000000000";
        public const string CrcHubTransferEventTopic = "0x8451019aab65b4193860ef723cb0d56b475a26a72b7bfc55c1dbd6121015285a";
        public const string CrcTrustEventTopic = "0xe60c754dd8ab0b1b5fccba257d6ebcd7d09e360ab7dd7a6e58198ca1f57cdcec";
        public const string TransferEventTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        public const string CrcSignupEventTopic = "0x358ba8f768af134eb5af120e9a61dc1ef29b29f597f047b555fc3675064a0342";
        public const string GnosisSafeOwnerAddedTopic = "";
        public const string CrcOrganisationSignupEventTopic = "0xb0b94cff8b84fc67513b977d68a5cdd67550bd9b8d99a34b570e3367b7843786";
        public const string GnosisSafeExecutionSuccessEventTopic = "0x442e715f626346e8c54381002da614f62bee8d27386535b2521ec8540898556e";
        public const string EmptyUInt256 = "0x0000000000000000000000000000000000000000000000000000000000000000";
        
        public static TransactionClass Classify(Transaction transaction, TransactionReceipt receipt, TransactionClass? externalClasses)
        {
            var logs = receipt.Logs;
            
            if (logs.Count == 0 && transaction.To == null)
            {
                return TransactionClass.ContractCreation;
            }
            if (logs.Count == 0 && transaction.Value.Value > 0)
            {
                return (externalClasses ?? TransactionClass.Unknown) | TransactionClass.EoaEthTransfer;
            }
            if (logs.Count == 0 && transaction.Value.Value == 0)
            {
                return (externalClasses ?? TransactionClass.Unknown) | TransactionClass.Call;
            }

            var transactionClass = TransactionClass.Unknown;
            
            var flatTopics = logs.SelectMany(log => log.SelectToken("topics").Values<string>()).ToHashSet();
            if (flatTopics.Count == 1 && flatTopics.Contains(GnosisSafeExecutionSuccessEventTopic))
            {
                return TransactionClass.GnosisSafeEthTransfer;
            }
            if (flatTopics.Contains(CrcHubTransferEventTopic))
            {
                transactionClass |= TransactionClass.CrcTransfer;
            }
            if (flatTopics.Contains(CrcTrustEventTopic))
            {
                transactionClass |= TransactionClass.CrcTrust;
            }
            if (logs.FirstOrDefault(log
                => log.SelectToken("topics").Values<string>().Contains(TransferEventTopic)
                   && log.SelectToken("topics").Values<string>().Contains(EmptyUInt256)) != null)
            {
                transactionClass |= TransactionClass.TokenMinting;
            }
            else if (flatTopics.Contains(TransferEventTopic))
            {
                transactionClass |= TransactionClass.Erc20Transfer;
            }
            if (flatTopics.Contains(CrcSignupEventTopic) 
                && flatTopics.Contains(CrcTrustEventTopic) 
                && flatTopics.Contains(TransferEventTopic))
            {
                transactionClass |= TransactionClass.CrcSignup;
            }
            if (flatTopics.Contains(CrcOrganisationSignupEventTopic))
            {
                transactionClass |= TransactionClass.CrcOrganisationSignup;
            }

            return externalClasses ?? TransactionClass.Unknown | transactionClass;
        }
    }
}
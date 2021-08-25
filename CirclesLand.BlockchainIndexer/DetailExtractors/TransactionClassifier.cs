using System.Linq;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;
using Newtonsoft.Json.Linq;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class TransactionClassifier
    {
        public const string HubAddress = "0x29b9a7fbb8995b2423a71cc17cf9810798f6c543";
        public const string AddressEmptyBytesPrefix = "0x000000000000000000000000";
        public const string CrcHubTransferEventTopic = "0x8451019aab65b4193860ef723cb0d56b475a26a72b7bfc55c1dbd6121015285a";
        public const string CrcTrustEventTopic = "0xe60c754dd8ab0b1b5fccba257d6ebcd7d09e360ab7dd7a6e58198ca1f57cdcec";
        public const string TransferEventTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        public const string CrcSignupEventTopic = "0x358ba8f768af134eb5af120e9a61dc1ef29b29f597f047b555fc3675064a0342";
        public const string GnosisSafeOwnerAddedTopic = "";
        public const string CrcOrganisationSignupEventTopic = "0xb0b94cff8b84fc67513b977d68a5cdd67550bd9b8d99a34b570e3367b7843786";
        public const string GnosisSafeExecutionSuccessEventTopic = "0x442e715f626346e8c54381002da614f62bee8d27386535b2521ec8540898556e";
        public const string EmptyUInt256 = "0x0000000000000000000000000000000000000000000000000000000000000000";
        public const string EmptyAddress = "0x0000000000000000000000000000000000000000";
        
        
        /// <summary>
        /// A crc signup transaction always has three logs: Signup, Trust and Transfer.
        /// </summary>
        public static bool IsCrcSignup( 
            TransactionReceipt receipt,
            out string? userAddress,
            out string? tokenAddress)
        {
            userAddress = null;
            tokenAddress = null;
            
            if (receipt.Logs.Count < 3)
            {
                return false;
            }

            //
            // Decode and check the Signup-log
            //
            var signupLog = receipt.Logs.SingleOrDefault(log => 
                log.SelectToken("topics").Values<string>().Contains(CrcSignupEventTopic));
            
            if (signupLog == null)
            {
                return false;
            }
            
            var signupLogAddress = signupLog.Value<string>("address");
            if (signupLogAddress != HubAddress)
            {
                return false;
            }

            var signupLogTopics = signupLog.SelectToken("topics").Values<string>().ToArray();
            if (signupLogTopics.Length != 2)
            {
                return false;
            }

            userAddress = signupLogTopics[1].Replace(AddressEmptyBytesPrefix, "0x");
            tokenAddress = signupLog.Value<string>("data").Replace(AddressEmptyBytesPrefix, "0x");
            
            //
            // Decode and check the Trust-log
            //
            var trustLog = receipt.Logs.SingleOrDefault(o => IsCrcTrust(o, out _, out _, out _));
            if (trustLog == null)
            {
                return false;
            }

            IsCrcTrust(
                trustLog, 
                out var canSendTo,
                out var user, 
                out var limit);
            
            if (canSendTo != user || canSendTo != userAddress || user != userAddress)
            {
                return false;
            }
            if (limit == null || limit.Value < 0 || limit.Value > 100)
            {
                return false;
            }
            
            //
            // Decode and check the Transfer-log
            //
            var erc20TransferLog = receipt.Logs.SingleOrDefault(o => IsErc20Transfer(o, out _, out _, out _, out _));
            if (erc20TransferLog == null)
            {
                return false;
            }
            
            IsErc20Transfer(
                erc20TransferLog, 
                out var transferTokenAddress,
                out var from, 
                out var to, 
                out var value);
            
            if (transferTokenAddress != tokenAddress)
            {
                return false;
            }
            if (from != EmptyAddress)
            {
                return false;
            }
            if (to != userAddress)
            {
                return false;
            }
            if (value == null || value.ToString() != "50000000000000000000")
            {
                return false;
            }

            return true;
        }

        public static bool IsCrcOrganisationSignup(
            JToken logEntry,
            out string? organisationAddress)
        {
            organisationAddress = null;

            var topics = logEntry.SelectToken("topics");
            if (topics == null)
            {
                return false;
            }
            if (!topics.Values<string>().Contains(CrcOrganisationSignupEventTopic))
            {
                return false;
            }
            
            var organisationSignupLogAddress = logEntry.Value<string>("address");
            if (organisationSignupLogAddress != HubAddress)
            {
                return false;
            }
            
            var organisationSignupLogTopics = topics.Values<string>().ToArray();
            if (organisationSignupLogTopics.Length != 2)
            {
                return false;
            }
            
            organisationAddress = organisationSignupLogTopics[1].Replace(AddressEmptyBytesPrefix, "0x");
            
            return true;
        }

        public static bool IsCrcHubTransfer(
            TransactionReceipt receipt,
            out string? from,
            out string? to,
            out HexBigInteger? amount)
        {
            from = null;
            to = null;
            amount = null;
            
            var hubTransferLog = receipt.Logs.SingleOrDefault(log => 
                log.SelectToken("topics").Values<string>().Contains(CrcHubTransferEventTopic));

            if (hubTransferLog == null)
            {
                return false;
            }
            
            var hubTransferLogAddress = hubTransferLog.Value<string>("address");
            if (hubTransferLogAddress != HubAddress)
            {
                return false;
            }
            
            var hubTransferLogTopics = hubTransferLog.SelectToken("topics").Values<string>().ToArray();
            if (hubTransferLogTopics.Length != 3)
            {
                return false;
            }
            
            from = hubTransferLogTopics[1].Replace(AddressEmptyBytesPrefix, "0x");
            to = hubTransferLogTopics[2].Replace(AddressEmptyBytesPrefix, "0x");
            var amountStr = hubTransferLog.Value<string>("data").Replace(AddressEmptyBytesPrefix, "0x");
            amount = new HexBigInteger(amountStr);

            // TODO: Check if "from" and "to" previously appeared in a CrcSignup
            
            // Hub transfers always appear together with at least one ERC20 transfer
            return receipt.Logs.Any(o => IsErc20Transfer(o, out _, out _, out _, out _));
        }

        public static bool IsCrcTrust(
            JToken logEntry,
            out string? canSendTo, 
            out string? user, 
            out HexBigInteger? limit)
        {
            canSendTo = null;
            user = null;
            limit = null;

            var topics = logEntry.SelectToken("topics");
            if (topics == null)
            {
                return false;
            }
            if (!topics.Values<string>().Contains(CrcTrustEventTopic))
            {
                return false;
            }
            
            var trustLogAddress = logEntry.Value<string>("address");
            if (trustLogAddress != HubAddress)
            {
                return false;
            }
            
            var trustLogTopics = topics.Values<string>().ToArray();
            if (trustLogTopics.Length != 3)
            {
                return false;
            }
            
            canSendTo = trustLogTopics[1].Replace(AddressEmptyBytesPrefix, "0x");
            user = trustLogTopics[2].Replace(AddressEmptyBytesPrefix, "0x");
            limit = new HexBigInteger(logEntry.Value<string>("data"));
            
            if (limit.Value < 0 || limit.Value > 100)
            {
                return false;
            }
            
            // TODO: Check if "canSendTo" and "user" previously appeared in a CrcSignup
            return true;
        }
        
        public static bool IsErc20Transfer(
            JToken logEntry,
            out string? tokenAddress,
            out string? from,
            out string? to,
            out HexBigInteger? value)
        {
            tokenAddress = null;
            from = null;
            to = null;
            value = null;

            var topics = logEntry.SelectToken("topics");
            if (topics == null)
            {
                return false;
            }
            
            if (!topics.Values<string>().Contains(TransferEventTopic))
            {
                return false;
            }
            
            var transferLogTopics = topics.Values<string>().ToArray();
            if (transferLogTopics.Length != 3)
            {
                return false;
            }

            tokenAddress = logEntry.Value<string>("address");
            from = transferLogTopics[1].Replace(AddressEmptyBytesPrefix, "0x");
            to = transferLogTopics[2].Replace(AddressEmptyBytesPrefix, "0x");
            var valueStr = logEntry.Value<string>("data").Replace(AddressEmptyBytesPrefix, "0x");
            value = new HexBigInteger(valueStr);
            
            return true;
        }
        
        public static TransactionClass Classify(Transaction transaction, TransactionReceipt receipt, TransactionClass? externalClasses)
        {
            var isErc20Transfer = receipt.Logs.Any(o => IsErc20Transfer(o, out _, out _, out _, out _));
            var isCrcSignup = IsCrcSignup(receipt, out _, out _);
            var isCrcOrganisationSignup = receipt.Logs.Any(o => IsCrcOrganisationSignup(o, out _));
            var isCrcHubTransfer = IsCrcHubTransfer(receipt, out _, out _, out _);
            var isCrcTrust = receipt.Logs.Any(o => IsCrcTrust(o, out _, out _, out _));

            var classification = TransactionClass.Unknown;
            if (isErc20Transfer)
            {
                classification |= TransactionClass.Erc20Transfer;
            }
            if (isCrcSignup)
            {
                classification |= TransactionClass.CrcSignup;
            }
            if (isCrcOrganisationSignup)
            {
                classification |= TransactionClass.CrcOrganisationSignup;
            }
            if (isCrcHubTransfer)
            {
                classification |= TransactionClass.CrcHubTransfer;
            }
            if (isCrcTrust)
            {
                classification |= TransactionClass.CrcTrust;
            }

            return classification;
        }
    }
}
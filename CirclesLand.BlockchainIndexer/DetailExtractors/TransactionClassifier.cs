using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.ABI.FunctionEncoding;
using Nethereum.ABI.Model;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;
using Newtonsoft.Json.Linq;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class TransactionClassifier
    {
        public static IEnumerable<string> GetTopics(JToken log)
        {
            var topicsArray = log.SelectToken("topics");
            return (IEnumerable<string>?)topicsArray?.Values<string>().Where(o => o != null) ?? Array.Empty<string>();
        }
        
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
                GetTopics(log).Contains(SettingsValues.CrcSignupEventTopic));

            if (signupLog == null)
            {
                return false;
            }

            var signupLogAddress = signupLog.Value<string>("address");
            if (signupLogAddress != SettingsValues.HubAddress)
            {
                return false;
            }

            var signupLogTopics = GetTopics(signupLog).ToArray();
            if (signupLogTopics.Length != 2)
            {
                return false;
            }

            userAddress = signupLogTopics[1].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            tokenAddress = signupLog.Value<string>("data")?.Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");

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

            if (from != SettingsValues.EmptyAddress)
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

            var topics = GetTopics(logEntry).ToArray();

            if (!topics.Contains(SettingsValues.CrcOrganisationSignupEventTopic))
            {
                return false;
            }

            var organisationSignupLogAddress = logEntry.Value<string>("address");
            if (organisationSignupLogAddress != SettingsValues.HubAddress)
            {
                return false;
            }

            if (topics.Length != 2)
            {
                return false;
            }

            organisationAddress = topics[1].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");

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
                GetTopics(log).Contains(SettingsValues.CrcHubTransferEventTopic));

            if (hubTransferLog == null)
            {
                return false;
            }

            var hubTransferLogAddress = hubTransferLog.Value<string>("address");
            if (hubTransferLogAddress != SettingsValues.HubAddress)
            {
                return false;
            }

            var hubTransferLogTopics = GetTopics(hubTransferLog).ToArray();
            if (hubTransferLogTopics.Length != 3)
            {
                return false;
            }

            from = hubTransferLogTopics[1].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            to = hubTransferLogTopics[2].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            var amountStr = hubTransferLog.Value<string>("data")?.Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            if (amountStr != null)
            {
                amount = new HexBigInteger(amountStr);
            }

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

            var topics = GetTopics(logEntry).ToArray();

            if (!topics.Contains(SettingsValues.CrcTrustEventTopic))
            {
                return false;
            }

            var trustLogAddress = logEntry.Value<string>("address");
            if (trustLogAddress != SettingsValues.HubAddress)
            {
                return false;
            }

            if (topics.Length != 3)
            {
                return false;
            }

            canSendTo = topics[1].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            user = topics[2].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
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

            var topics = GetTopics(logEntry).ToArray();

            if (!topics.Contains(SettingsValues.TransferEventTopic))
            {
                return false;
            }

            if (topics.Length != 3)
            {
                return false;
            }

            tokenAddress = logEntry.Value<string>("address");
            from = topics[1].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            to = topics[2].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            var valueStr = logEntry.Value<string>("data")?.Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            if (valueStr != null)
            {
                value = new HexBigInteger(valueStr);
            }
            
            return true;
        }

        public static bool IsSafeEthTransfer(
            Transaction transaction,
            TransactionReceipt receipt,
            out string? initiator,
            out string? from,
            out string? to,
            out HexBigInteger? value)
        {
            initiator = null;
            from = null;
            to = null;
            value = null;

            if (!transaction.Input.StartsWith(SettingsValues.ExecTransactionMethodId))
            {
                return false;
            }

            var decoded = new FunctionCallDecoder()
                .DecodeFunctionInput(
                    "0x6a761202",
                    transaction.Input,
                    new Parameter("address", 0),
                    new Parameter("uint256", 1),
                    new Parameter("bytes", 2),
                    new Parameter("uint8", 3),
                    new Parameter("uint256", 4),
                    new Parameter("uint256", 5),
                    new Parameter("uint256", 6),
                    new Parameter("address", 7),
                    new Parameter("address", 8),
                    new Parameter("bytes", 9));

            initiator = transaction.From.ToLower();
            from = transaction.To.ToLower();
            to = (decoded[0].Result as string)?.ToLower();
            var v = decoded[1].Result as BigInteger?;
            if (v == null)
            {
                return false;
            }

            value = new HexBigInteger(v.Value);

            var data = decoded[2].Result as byte[];
            if (data == null || data.Length > 0)
            {
                return false;
            }

            var operation = decoded[3].Result as BigInteger?;
            if (operation != 0)
            {
                return false;
            }
            
            var executionSuccessLog = receipt.Logs.SingleOrDefault(log =>
                GetTopics(log).Contains(SettingsValues.ExecutionSuccessEventTopic));
            
            return executionSuccessLog != null;
        }

        public static bool IsEoaEthTransfer (
            Transaction transaction,
            TransactionReceipt receipt,
            out string? from,
            out string? to,
            out HexBigInteger? value)
        {
            from = null;
            to = null;
            value = null;

            if (transaction.Value.Value == 0)
            {
                return false;
            }

            if (receipt.Logs.Count > 0)
            {
                return false;
            }

            if (transaction.Input != "0x")
            {
                return false;
            }

            if (transaction.To == null)
            {
                return false;
            }

            from = transaction.From;
            to = transaction.To;
            value = transaction.Value;

            return true;
        }

        public static TransactionClass Classify(Transaction transaction, TransactionReceipt receipt,
            TransactionClass? externalClasses)
        {
            try
            {
                var isEoaEthTransfer = IsEoaEthTransfer(transaction, receipt, out _, out _, out _);
                if (!isEoaEthTransfer && receipt.Logs == null)
                {
                    return TransactionClass.Unknown;
                }

                var isErc20Transfer = receipt.Logs.Any(o => IsErc20Transfer(o, out _, out _, out _, out _));
                var isCrcSignup = IsCrcSignup(receipt, out _, out _);
                var isCrcOrganisationSignup = receipt.Logs.Any(o => IsCrcOrganisationSignup(o, out _));
                var isCrcHubTransfer = IsCrcHubTransfer(receipt, out _, out _, out _);
                var isCrcTrust = receipt.Logs.Any(o => IsCrcTrust(o, out _, out _, out _));
                var isSafeEthTransfer = IsSafeEthTransfer(transaction, receipt, out _, out _, out _, out _);


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

                if (isSafeEthTransfer)
                {
                    classification |= TransactionClass.SafeEthTransfer;
                }

                if (isSafeEthTransfer)
                {
                    classification |= TransactionClass.SafeEthTransfer;
                }

                if (isEoaEthTransfer)
                {
                    classification |= TransactionClass.EoaEthTransfer;
                }

                return classification;
            } 
            catch (Exception e)
            {
                Console.WriteLine(e);
                return TransactionClass.Unknown;
            }
        }
    }
}
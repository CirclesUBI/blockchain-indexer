using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethermind.Core;
using Nethermind.Int256;

namespace CirclesLand.BlockchainIndexer.NethermindPlugin;

public static class TransactionClassifier
{
        
        /// <summary>
        /// A crc signup transaction always has three logs: Signup, Trust and Transfer.
        /// </summary>
        public static bool IsCrcSignup(
            TxReceipt receipt,
            out string? userAddress,
            out string? tokenAddress)
        {
            userAddress = null;
            tokenAddress = null;

            if (receipt.Logs.Length < 3)
            {
                return false;
            }

            //
            // Decode and check the Signup-log
            //
            var signupLog = receipt.Logs.SingleOrDefault(log =>
                log.Topics.Select(o => o.ToString()).Contains(SettingsValues.CrcSignupEventTopic));

            if (signupLog == null)
            {
                return false;
            }

            var signupLogAddress = signupLog.LoggersAddress.ToString(false);
            if (signupLogAddress != SettingsValues.HubAddress)
            {
                return false;
            }

            var signupLogTopics = signupLog.Topics.Select(o => o.ToString()).ToArray();
            if (signupLogTopics.Length != 2)
            {
                return false;
            }

            userAddress = signupLogTopics[1].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            tokenAddress = new Address(signupLog.Data).ToString(false);

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
            LogEntry logEntry,
            out string? organisationAddress)
        {
            organisationAddress = null;

            var topics = logEntry.Topics.Select(o => o.ToString()).ToArray();

            if (!topics.Contains(SettingsValues.CrcOrganisationSignupEventTopic))
            {
                return false;
            }

            var organisationSignupLogAddress = logEntry.LoggersAddress.ToString(false);
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
            TxReceipt receipt,
            out string? from,
            out string? to,
            out UInt256? amount)
        {
            from = null;
            to = null;
            amount = null;

            var hubTransferLog = receipt.Logs.SingleOrDefault(log =>
                log.Topics
                    .Select(o => o.ToString())
                    .Contains(SettingsValues.CrcHubTransferEventTopic));

            if (hubTransferLog == null)
            {
                return false;
            }

            var hubTransferLogAddress = hubTransferLog.LoggersAddress.ToString(false);
            if (hubTransferLogAddress != SettingsValues.HubAddress)
            {
                return false;
            }

            var hubTransferLogTopics = hubTransferLog.Topics.Select(o => o.ToString()).ToArray();
            if (hubTransferLogTopics.Length != 3)
            {
                return false;
            }

            from = hubTransferLogTopics[1].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            to = hubTransferLogTopics[2].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            amount = new UInt256(hubTransferLog.Data);
            

            // TODO: Check if "from" and "to" previously appeared in a CrcSignup
            // Hub transfers always appear together with at least one ERC20 transfer
            return receipt.Logs.Any(o => IsErc20Transfer(o, out _, out _, out _, out _));
        }

        public static bool IsCrcTrust(
            LogEntry logEntry,
            out string? canSendTo,
            out string? user,
            out UInt256? limit)
        {
            canSendTo = null;
            user = null;
            limit = null;

            var topics = logEntry.Topics.Select(o => o.ToString()).ToArray();
            if (!topics.Contains(SettingsValues.CrcTrustEventTopic))
            {
                return false;
            }

            var trustLogAddress = logEntry.LoggersAddress.ToString(false);
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
            limit = new UInt256(logEntry.Data);

            if (limit.Value < 0 || limit.Value > 100)
            {
                return false;
            }

            // TODO: Check if "canSendTo" and "user" previously appeared in a CrcSignup
            return true;
        }

        public static bool IsErc20Transfer(
            LogEntry logEntry,
            out string? tokenAddress,
            out string? from,
            out string? to,
            out UInt256? value)
        {
            tokenAddress = null;
            from = null;
            to = null;
            value = null;

            var topics = logEntry.Topics.Select(o => o.ToString()).ToArray();

            if (!topics.Contains(SettingsValues.TransferEventTopic))
            {
                return false;
            }

            if (topics.Length != 3)
            {
                return false;
            }

            tokenAddress = logEntry.LoggersAddress.ToString(false);
            from = topics[1].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            to = topics[2].Replace(SettingsValues.AddressEmptyBytesPrefix, "0x");
            value = new UInt256(logEntry.Data);
            
            return true;
        }

        public static bool IsEoaEthTransfer (
            Transaction transaction,
            TxReceipt receipt,
            out string? from,
            out string? to,
            out UInt256? value)
        {
            from = null;
            to = null;
            value = null;

            if (transaction.Value == 0)
            {
                return false;
            }

            if (receipt.Logs?.Length > 0)
            {
                return false;
            }

            if (transaction.Data?.Length > 0)
            {
                return false;
            }

            if (transaction.To == null)
            {
                return false;
            }

            if (transaction.SenderAddress == null)
            {
                return false;
            }

            from = transaction.SenderAddress.ToString(false);
            to = transaction.To.ToString(false);
            value = transaction.Value;

            return true;
        }

        public static TransactionClass Classify(Transaction transaction, TxReceipt receipt,
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
using System;
using System.Collections.Generic;
using System.Linq;
using Npgsql;

namespace CirclesLand.BlockchainIndexer
{
    public static class SettingsValues
    {
        public static readonly string AddressEmptyBytesPrefix = "0x000000000000000000000000";

        public static readonly string CrcHubTransferEventTopic =
            "0x8451019aab65b4193860ef723cb0d56b475a26a72b7bfc55c1dbd6121015285a";

        public static readonly string CrcTrustEventTopic = "0xe60c754dd8ab0b1b5fccba257d6ebcd7d09e360ab7dd7a6e58198ca1f57cdcec";
        public static readonly string TransferEventTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        public static readonly string CrcSignupEventTopic = "0x358ba8f768af134eb5af120e9a61dc1ef29b29f597f047b555fc3675064a0342";
        public static readonly string GnosisSafeOwnerAddedTopic = "";

        public static readonly string CrcOrganisationSignupEventTopic =
            "0xb0b94cff8b84fc67513b977d68a5cdd67550bd9b8d99a34b570e3367b7843786";

        public static readonly string GnosisSafeExecutionSuccessEventTopic =
            "0x442e715f626346e8c54381002da614f62bee8d27386535b2521ec8540898556e";

        public static readonly string EmptyUInt256 = "0x0000000000000000000000000000000000000000000000000000000000000000";
        public static readonly string EmptyAddress = "0x0000000000000000000000000000000000000000";
        public static readonly string ExecTransactionMethodId = "0x6a761202";

        public static readonly string ExecutionSuccessEventTopic =
            "0x442e715f626346e8c54381002da614f62bee8d27386535b2521ec8540898556e";
        
        public static string ConnectionString;
        public static string RpcEndpointUrl;
        public static string? RpcWsEndpointUrl;
        public static string WebsocketServerUrl;
        public static int DelayStartup;
        public static long StartFromBlock;
        public static int UseBulkSourceThreshold;

        /// <summary>
        /// Specifies after how many imported blocks the import_from_staging_tables() procedure should be called.
        /// </summary>
        /// <remarks></remarks>
        public static int BulkFlushInterval;

        public static int BulkFlushTimeoutInSeconds;
        public static int SerialFlushInterval;
        public static int SerialFlushTimeoutInSeconds;
        public static int ErrorRestartPenaltyInMs;
        public static int MaxErrorRestartPenaltyInMs;
        public static int PollingIntervalInMs;
        public static int MaxParallelBlockDownloads;
        public static int MaxDownloadedBlockBufferSize;
        public static int MaxParallelReceiptDownloads;
        public static int MaxDownloadedTransactionsBufferSize;
        public static int MaxDownloadedReceiptsBufferSize;
        public static int WriteToStagingBatchSize;
        public static int WriteToStagingBatchMaxIntervalInSeconds;
        public static int MaxWriteToStagingBatchBufferSize;
        public static string HubAddress;
        public static string MultiSendContractAddress;
        
        public static readonly Dictionary<string, string> InvalidSettings = new();
        public static readonly Dictionary<string, (string, bool)> ValidSettings = new();
    }

    public static class Settings
    {
        static int TryGetIntEnvVar(string variableName, int defaultValue)
        {
            var val = Environment.GetEnvironmentVariable(variableName);
            var isInt = int.TryParse(val?.Trim(), out var i);
            if (val != null && !isInt)
            {
                SettingsValues.InvalidSettings.Add(variableName, val);
            }
            var returnVal = isInt ? i : defaultValue;
            SettingsValues.ValidSettings.Add(variableName, (returnVal.ToString(), val == null));
            return returnVal;
        }

        static string TryGetStringEnvVar(string variableName, string defaultValue)
        {
            var val = Environment.GetEnvironmentVariable(variableName);
            var returnVal = val ?? defaultValue;
            SettingsValues.ValidSettings.Add(variableName, (returnVal, val == null));
            return returnVal;
        }

        static long TryGetLongEnvVar(string variableName, long defaultValue)
        {
            var val = Environment.GetEnvironmentVariable(variableName);
            var isInt = long.TryParse(val?.Trim(), out var i);
            if (val != null && !isInt)
            {
                SettingsValues.InvalidSettings.Add(variableName, val);
            }
            var returnVal = isInt ? i : defaultValue;
            SettingsValues.ValidSettings.Add(variableName, (returnVal.ToString(), val == null));
            return returnVal;
        }
        
        static Settings()
        {
            var validationErrors = new List<string>();
            var connectionString = Environment.GetEnvironmentVariable("INDEXER_CONNECTION_STRING");
            try
            {
                var csb = new NpgsqlConnectionStringBuilder(connectionString);
                if (string.IsNullOrWhiteSpace(csb.Host))
                    validationErrors.Add("The INDEXER_CONNECTION_STRING contains no 'Server'");
                if (string.IsNullOrWhiteSpace(csb.Username))
                    validationErrors.Add("The INDEXER_CONNECTION_STRING contains no 'User ID'");
                if (string.IsNullOrWhiteSpace(csb.Database))
                    validationErrors.Add("The INDEXER_CONNECTION_STRING contains no 'Database'");
            }
            catch (Exception ex)
            {
                validationErrors.Add("The INDEXER_CONNECTION_STRING is not valid:");
                validationErrors.Add(ex.Message);
            }

            if (!Uri.TryCreate(Environment.GetEnvironmentVariable("INDEXER_RPC_GATEWAY_URL"), UriKind.Absolute, 
                    out var rpcGatewayUri))
            {
                validationErrors.Add("Couldn't parse the 'INDEXER_RPC_GATEWAY_URL' environment variable. Expected 'System.Uri'.");
            }

            if (!Uri.TryCreate(Environment.GetEnvironmentVariable("INDEXER_RPC_GATEWAY_WS_URL"), UriKind.Absolute, 
                    out var rpcGatewayWsUri))
            {
                if (rpcGatewayUri == null)
                {
                    validationErrors.Add(
                        "Couldn't parse the 'INDEXER_RPC_GATEWAY_WS_URL' environment variable. Expected 'System.Uri'.");
                }
            }

            if (!Uri.TryCreate(Environment.GetEnvironmentVariable("INDEXER_WEBSOCKET_URL"), UriKind.Absolute,
                out var websocketUrl))
            {
                validationErrors.Add("Couldn't parse the 'INDEXER_WEBSOCKET_URL' environment variable. Expected 'System.Uri'.");
            }

            if (validationErrors.Count > 0)
            {
                throw new ArgumentException(string.Join(Environment.NewLine, validationErrors));
            }

            SettingsValues.ConnectionString = connectionString ?? "null";
            SettingsValues.ValidSettings.Add("INDEXER_CONNECTION_STRING", ("hidden", false));

            SettingsValues.RpcEndpointUrl = rpcGatewayUri?.ToString() ?? "null";
            SettingsValues.ValidSettings.Add("INDEXER_RPC_GATEWAY_URL", (SettingsValues.RpcEndpointUrl, false));

            SettingsValues.RpcWsEndpointUrl = rpcGatewayWsUri?.ToString() ?? "null";
            SettingsValues.ValidSettings.Add("INDEXER_RPC_GATEWAY_WS_URL", (SettingsValues.RpcWsEndpointUrl, false));

            SettingsValues.WebsocketServerUrl = websocketUrl?.ToString() ?? "null";
            SettingsValues.ValidSettings.Add("INDEXER_WEBSOCKET_URL", (SettingsValues.WebsocketServerUrl, false));

            SettingsValues.UseBulkSourceThreshold = TryGetIntEnvVar("USE_BULK_SOURCE_THRESHOLD", 24);
            SettingsValues.BulkFlushInterval = TryGetIntEnvVar("BULK_FLUSH_INTERVAL_IN_BLOCKS", 10);
            SettingsValues.BulkFlushTimeoutInSeconds = TryGetIntEnvVar("BULK_FLUSH_TIMEOUT_IN_SECONDS", 240);
            SettingsValues.SerialFlushInterval = TryGetIntEnvVar("SERIAL_FLUSH_INTERVAL_IN_BLOCKS",1 );
            SettingsValues.SerialFlushTimeoutInSeconds = TryGetIntEnvVar("SERIAL_FLUSH_TIMEOUT_IN_SECONDS", 10);
            SettingsValues.ErrorRestartPenaltyInMs = TryGetIntEnvVar("ERROR_RESTART_PENALTY_IN_MILLISECONDS", 1000 * 5);
            SettingsValues.MaxErrorRestartPenaltyInMs = TryGetIntEnvVar("MAX_ERROR_RESTART_PENALTY_IN_MILLISECONDS", 1000 * 60 * 4);
            SettingsValues.PollingIntervalInMs = TryGetIntEnvVar("POLLING_INTERVAL_IN_MILLISECONDS", 500);
            SettingsValues.MaxParallelBlockDownloads = TryGetIntEnvVar("MAX_PARALLEL_BLOCK_DOWNLOADS", 24);
            SettingsValues.MaxDownloadedBlockBufferSize = TryGetIntEnvVar("MAX_BLOCK_BUFFER_SIZE", SettingsValues.MaxParallelBlockDownloads * 25);
            SettingsValues.MaxParallelReceiptDownloads = TryGetIntEnvVar("MAX_PARALLEL_RECEIPT_DOWNLOADS", 96);
            SettingsValues.MaxDownloadedTransactionsBufferSize = TryGetIntEnvVar("MAX_TRANSACTION_BUFFER_SIZE", SettingsValues.MaxParallelReceiptDownloads * 25);
            SettingsValues.MaxDownloadedReceiptsBufferSize = TryGetIntEnvVar("MAX_RECEIPT_BUFFER_SIZE", 96 * 25);
            SettingsValues.WriteToStagingBatchSize = TryGetIntEnvVar("WRITE_TO_STAGING_BATCH_SIZE", 2000);
            SettingsValues.WriteToStagingBatchMaxIntervalInSeconds = TryGetIntEnvVar("WRITE_TO_STAGING_BATCH_MAX_INTERVAL_IN_SECONDS", 5);
            SettingsValues.MaxWriteToStagingBatchBufferSize = TryGetIntEnvVar("MAX_WRITE_TO_STAGING_BATCH_BUFFER_SIZE", 2048);
            SettingsValues.StartFromBlock = TryGetLongEnvVar("START_FROM_BLOCK", 12529458L);
            SettingsValues.HubAddress = TryGetStringEnvVar("HUB_ADDRESS", "0x29b9a7fbb8995b2423a71cc17cf9810798f6c543").ToLowerInvariant();
            SettingsValues.DelayStartup = TryGetIntEnvVar("DELAY_START", 0);

            if (string.IsNullOrWhiteSpace(SettingsValues.HubAddress))
            {
                throw new Exception("Cannot start without a valid configured HUB_ADDRESS.");
            }

            SettingsValues.MultiSendContractAddress = TryGetStringEnvVar("MULTI_SEND_ADDRESS", "");
            

            Console.WriteLine("Configuration: ");
            Console.WriteLine("-------------------------------------------");
            foreach (var (key, value) in SettingsValues.InvalidSettings)
            {
                Console.WriteLine($"ERR: The value of environment variable '{key}' is invalid: {value}");
            }

            if (SettingsValues.InvalidSettings.Count > 0)
            {
                Console.WriteLine("-------------------------------------------");
                throw new Exception("Invalid configuration");
            }
            
            var col1Width = SettingsValues.ValidSettings.Select(o => o.Key).Max(o => o.Length) + 2;
            var col2Width = SettingsValues.ValidSettings.Select(o => o.Value.Item1).Max(o => o.Length) + 2;
            
            foreach (var (key, value) in SettingsValues.ValidSettings)
            {
                var formattedKey = (key + ": ").PadRight(col1Width);
                var formattedVal = value.Item1.PadRight(col2Width);
                var defaultIndicator = value.Item2 ? "(default)" : "(environment variable)";
                
                Console.WriteLine($"{formattedKey}{formattedVal} {defaultIndicator}");
            }
            
            Console.WriteLine("-------------------------------------------");
        }
    }
}
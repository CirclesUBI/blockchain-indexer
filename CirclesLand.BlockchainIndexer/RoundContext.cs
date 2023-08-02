using System;
using System.Data;
using System.Net.Http.Json;
using System.Threading;
using System.Threading.Tasks;
using CirclesLand.BlockchainIndexer.Api;
using CirclesLand.BlockchainIndexer.Persistence;
using CirclesLand.BlockchainIndexer.Sources;
using CirclesLand.BlockchainIndexer.Util;
using Nethereum.Hex.HexTypes;
using Nethereum.JsonRpc.WebSocketClient;
using Nethereum.Web3;
using Newtonsoft.Json;
using Npgsql;

namespace CirclesLand.BlockchainIndexer
{
    public class RoundContext : IDisposable
    {
        public class RoundErrorEventArgs : EventArgs
        {
            public Exception Exception { get; }

            public RoundErrorEventArgs(Exception exception)
            {
                Exception = exception;
            }
        }

        public DateTime CreatedAt { get; }
        public DateTime StartAt { get; }
        public long RoundNo { get; }
        public NpgsqlConnection Connection { get; }
        public Web3? Web3 { get; private set;  }
        public SourceFactory SourceFactory { get; }

        public event EventHandler<RoundErrorEventArgs>? Error;
        public event EventHandler? Disposed;
        public event EventHandler? BatchSuccess;

        public RoundContext(long number, NpgsqlConnection connection, TimeSpan penalty)
        {
            RoundNo = number;
            CreatedAt = DateTime.Now;
            StartAt = CreatedAt + penalty;
            Connection = connection;
            SourceFactory = new SourceFactory();
        }

        public void Log(string message)
        {
            Logger.Log($"Round {RoundNo}: {message}");
        }

        public long GetLastValidBlock()
        {
            return BlockTracker.GetLastValidBlock(Connection, SettingsValues.StartFromBlock);
        }

        public void OnError(Exception exception)
        {
            Logger.LogError($"Round {RoundNo}: {exception.Message}");
            Logger.LogError($"Round {RoundNo}: {exception.StackTrace}");

            Error?.Invoke(this, new RoundErrorEventArgs(exception));
        }

        public void OnBatchSuccess()
        {
            BatchSuccess?.Invoke(this, EventArgs.Empty);
            Interlocked.Increment(ref Statistics.TotalProcessedBatches);

            //WebsocketService.BroadcastMessage(transactionsJson);
        }

        public void OnBatchSuccessNotify(string[] writtenTransactions)
        {
            OnBatchSuccess();
            
            Console.WriteLine($"Imported {writtenTransactions.Length} transactions");
            TransactionHashBroadcastService.BroadcastMessage(JsonConvert.SerializeObject(writtenTransactions));
        }

        public void Dispose()
        {
            Connection.Dispose();
            Disposed?.Invoke(this, EventArgs.Empty);
        }

        public async Task<HexBigInteger> Start(long lastPersistedBlock)
        {
            var tmpWeb3 = new Web3(SettingsValues.RpcEndpointUrl);
            var currentBlock = await tmpWeb3
                .Eth
                .Blocks
                .GetBlockNumber
                .SendRequestAsync();
            
            if (currentBlock == null)
            {
                throw new Exception("Couldn't request the most recent block from the rpc gateway.");
            }
            
            // Always use http in the first round because it allows for more parallel downloads.
            // Use websockets afterwards because of the lower latency.
            var delta = currentBlock.Value - lastPersistedBlock;
            
            var catchUpMostLikelyCompleted = 
                RoundNo > 1 
                && Statistics.TotalErrorCount < RoundNo
                && delta < SettingsValues.UseBulkSourceThreshold;
            
            if (!catchUpMostLikelyCompleted 
                || String.IsNullOrEmpty(SettingsValues.RpcWsEndpointUrl) 
                || SettingsValues.RpcWsEndpointUrl == "null")
            {
                Logger.Log("Using the http connection for the next round.");
                Web3 = new Web3(SettingsValues.RpcEndpointUrl); 
            }
            else
            {
                Logger.Log("Using the websocket connection for the next round.");
                var client = new WebSocketClient(SettingsValues.RpcWsEndpointUrl);
                Web3 = new Web3(client);
            }

            return currentBlock;
        }
    }
}
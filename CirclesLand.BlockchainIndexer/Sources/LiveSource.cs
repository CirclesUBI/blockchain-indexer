using System;
using System.Threading.Tasks;
using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using CirclesLand.BlockchainIndexer.Util;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.JsonRpc.Client.Streaming;
using Nethereum.JsonRpc.WebSocketStreamingClient;
using Nethereum.RPC.Eth.DTOs;
using Nethereum.RPC.Eth.Subscriptions;
using Nethereum.Web3;
using Newtonsoft.Json;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Sources;

public static class LiveSource
{
    public static Task<Source<HexBigInteger, NotUsed>> Create(string connectionString, string rpcUrl, long lastPersistedBlock)
    {
        var catchingUp = true;
            
        return Task.FromResult(Source.UnfoldAsync(new HexBigInteger(0), async lastBlock =>
        {
            await using var dbConnection = new NpgsqlConnection(connectionString);
            dbConnection.Open();

            var web3 = new Web3(rpcUrl);

            while (catchingUp)
            {
                try
                {
                    // Determine if we need to catch up (database old)
                    var mostRecentBlock = await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync();;

                    if (lastBlock.Value == 0)
                    {
                        lastBlock = new HexBigInteger(
                            lastPersistedBlock == 0 ? SettingsValues.StartFromBlock : lastPersistedBlock);
                    }

                    if (mostRecentBlock.ToLong() > lastPersistedBlock && mostRecentBlock.Value > lastBlock.Value)
                    {
                        var nextBlockToIndex = lastBlock.Value + 1;
                        Console.WriteLine($"Catching up block: {nextBlockToIndex}");

                        SourceMetrics.BlocksEmitted.WithLabels("live").Inc();
                        
                        return Option<(HexBigInteger, HexBigInteger)>.Create((new HexBigInteger(nextBlockToIndex),
                            new HexBigInteger(nextBlockToIndex)));
                    }

                    catchingUp = false;
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex.Message);
                    if (ex.StackTrace != null) Logger.LogError(ex.StackTrace);

                    throw;
                }
            }
            
            await dbConnection.CloseAsync();
            
            using var client = new StreamingWebSocketClient(SettingsValues.RpcWsEndpointUrl);
            var subscription = new EthNewBlockHeadersSubscription(client);
            
            var completionSource = new TaskCompletionSource<HexBigInteger>(TaskCreationOptions.None);

#pragma warning disable CS4014
            Task.Delay(TimeSpan.FromSeconds(20))
                .ContinueWith(_ =>
#pragma warning restore CS4014
                {
                    if (completionSource.Task.IsCompleted)
                    {
                        return;
                    }
                    completionSource.SetException(new TimeoutException("Received no new block from the LiveSource for 20 sec."));
                });

            var handler = new EventHandler<StreamingEventArgs<Block>>((sender, e) =>
            {
                if (e.Exception != null)
                {
                    completionSource.SetException(e.Exception);
                }
                else
                {
                    var utcTimestamp = DateTimeOffset.FromUnixTimeSeconds((long) e.Response.Timestamp.Value);
                    Console.WriteLine(
                        $"New Block: Number: {e.Response.Number.Value}, " +
                        $"Timestamp: {JsonConvert.SerializeObject(utcTimestamp)}, " +
                        $"Server time: {JsonConvert.SerializeObject(DateTime.Now.ToUniversalTime())}");

                    completionSource.SetResult(new HexBigInteger(e.Response.Number.HexValue));
                }
            });
            var errorHandler = new WebSocketStreamingErrorEventHandler((sender, exception) =>
            {
                Logger.LogError("RPC client websocket connection closed." + exception.Message);
                completionSource.SetException(exception);
            });

            subscription.SubscriptionDataResponse += handler;
            client.Error += errorHandler;
            
            await client.StartAsync();
            await subscription.SubscribeAsync();
            
            var currentBlock = await completionSource.Task;
            Statistics.TrackBlockEnter(currentBlock.ToLong());
                
            subscription.SubscriptionDataResponse -= handler;
            client.Error -= errorHandler;

            if (currentBlock.Value - 1 > lastBlock.Value)
            {
                throw new Exception($"The live source missed at least one block. Current block: {currentBlock.Value}; Last block: {lastBlock.Value}");
            }

            SourceMetrics.BlocksEmitted.WithLabels("live").Inc();
            
            return Option<(HexBigInteger, HexBigInteger)>.Create((currentBlock, currentBlock));  
        }));
    }
}
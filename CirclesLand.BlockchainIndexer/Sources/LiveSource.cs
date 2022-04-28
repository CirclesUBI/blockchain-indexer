using System;
using System.Threading.Tasks;
using Akka;
using Akka.IO;
using Akka.Streams.Dsl;
using Akka.Util;
using CirclesLand.BlockchainIndexer.Util;
using Dapper;
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

public class LiveSource
{
    public static async Task<Source<HexBigInteger, NotUsed>> Create(string connectionString, string rpcUrl)
    {
        var client = new StreamingWebSocketClient(rpcUrl.Replace("https://", "wss://") + "/ws");
        var subscription = new EthNewBlockHeadersSubscription(client);
            
        await client.StartAsync();
        await subscription.SubscribeAsync();

        var catchingUp = true;
            
        return Source.UnfoldAsync(new HexBigInteger(0), async lastBlock =>
        {
            await using var connection = new NpgsqlConnection(connectionString);
            connection.Open();

            var web3 = new Web3(rpcUrl);

            while (catchingUp)
            {
                try
                {
                    // Determine if we need to catch up (database old)
                    var mostRecentBlock = await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync();
                    var lastIndexedBlock =
                        connection.QuerySingleOrDefault<long?>("select max(number) from block") ?? 0;

                    if (lastBlock.Value == 0)
                    {
                        lastBlock = new HexBigInteger(
                            lastIndexedBlock == 0 ? Settings.StartFromBlock : lastIndexedBlock);
                    }

                    if (mostRecentBlock.ToLong() > lastIndexedBlock && mostRecentBlock.Value > lastBlock.Value)
                    {
                        var nextBlockToIndex = lastBlock.Value + 1;
                        Console.WriteLine($"Catching up block: {nextBlockToIndex}");

                        return new Option<(HexBigInteger, HexBigInteger)>((new HexBigInteger(nextBlockToIndex),
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

            var completionSource = new TaskCompletionSource<HexBigInteger>(TaskCreationOptions.None);
            var handler = new EventHandler<StreamingEventArgs<Block>>((sender, e) =>
            {
                var utcTimestamp = DateTimeOffset.FromUnixTimeSeconds((long) e.Response.Timestamp.Value);
                Console.WriteLine(
                    $"New Block: Number: {e.Response.Number.Value}, Timestamp: {JsonConvert.SerializeObject(utcTimestamp)}");
                    
                completionSource.SetResult(new HexBigInteger(e.Response.Number.HexValue));
            });

            subscription.SubscriptionDataResponse += handler;
            subscription.UnsubscribeResponse += (sender, e) =>
            {
                Logger.LogError("RPC client websocket connection closed.");
            };
                
            var currentBlock = await completionSource.Task;
                
            subscription.SubscriptionDataResponse -= handler;

            return new Option<(HexBigInteger, HexBigInteger)>((currentBlock, currentBlock));     
        });
    }
}
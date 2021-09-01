using System;
using System.Threading.Tasks;
using Nethereum.JsonRpc.Client.Streaming;
using Nethereum.JsonRpc.WebSocketStreamingClient;
using Nethereum.RPC.Eth.DTOs;
using Nethereum.RPC.Eth.Subscriptions;
using Newtonsoft.Json;

namespace CirclesLand.BlockchainIndexer.Sources
{
    public class LiveSource
    {
        public LiveSource()
        {
            
        }
        public static async Task NewBlockHeader_With_Subscription(string rpc)
        {
            using(var client = new StreamingWebSocketClient(rpc))
            { 
                // create a subscription 
                // it won't do anything just yet though
                var subscription = new EthNewBlockHeadersSubscription(client);

                // attach our handler for new block header data
                subscription.SubscriptionDataResponse += (sender, e) =>
                {
                    var utcTimestamp = DateTimeOffset.FromUnixTimeSeconds((long)e.Response.Timestamp.Value);
                    Console.WriteLine($"New Block: Number: {e.Response.Number.Value}, Timestamp: {JsonConvert.SerializeObject(utcTimestamp)}");
                };

                // open the web socket connection
                await client.StartAsync();

                // subscribe to new block headers
                // blocks will be received on another thread
                // therefore this doesn't block the current thread
                await subscription.SubscribeAsync();

                //allow some time before we close the connection and end the subscription
                await Task.Delay(TimeSpan.FromMinutes(1));

                // the connection closing will end the subscription
            }
        }
    }
}
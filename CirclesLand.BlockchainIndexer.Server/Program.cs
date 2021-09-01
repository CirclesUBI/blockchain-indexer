using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using CirclesLand.BlockchainIndexer.Api;
using CirclesLand.BlockchainIndexer.Util;
using Dapper;
using KestrelWebSocketServer;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Server
{
    public class Program
    {
        const string RpcUrl = "wss://rpc.circles.land/";

        public const string ConnectionString =
            @"Server=localhost;Port=5432;Database=circles_land_worker;User ID=postgres;Password=postgres;";

        public static async Task Main(string[] args)
        {
            var indexer = new Indexer(ConnectionString, RpcUrl);
            indexer.NewBlock += (s, e) =>
            {
                Task.Run(() =>
                {
                    try
                    {
                        using var connection = new NpgsqlConnection(ConnectionString);
                        connection.Open();

                        var safes = connection.Query(
                            @"select timestamp 
                                  , block_number::text
                                  , transaction_index
                                  , transaction_hash
                                  , type
                                  , safe_address
                                  , direction
                                  , value::text
                                  , obj::text as payload
                                 from crc_safe_timeline 
                                 where block_number = @block_number",
                            new
                            {
                                block_number = (long) e.Block.Value
                            });

                        var changes = safes.Select(o =>
                        {
                            o.payload = JObject.Parse(o.payload);
                            return o;
                        }).ToArray();

                        if (changes.Length == 0)
                        {
                            return;
                        };
                        
                        var msg = JsonConvert.SerializeObject(changes);
                        Logger.Log(msg);
                        WebsocketServer.BroadcastMessage(msg);
                    }
                    catch (Exception e)
                    {
                        Logger.LogError(e.Message);
                        Logger.LogError(e.StackTrace);
                    }
                });
            };
            
            indexer.Run();

            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseUrls(@"http://localhost:8080/");
                    webBuilder.UseStartup<Startup>();
                })
                .Build()
                .Run();
        }
    }
}
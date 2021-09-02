using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
        public static async Task Main(string[] args)
        {
            var validationErrors = new List<string>();
            var connectionString = Environment.GetEnvironmentVariable("INDEXER_CONNECTION_STRING");
            try
            {
                var csb = new NpgsqlConnectionStringBuilder(connectionString);
                if (string.IsNullOrWhiteSpace(csb.Host))
                    validationErrors.Add("The connection string contains no 'Server'");
                if (string.IsNullOrWhiteSpace(csb.Username))
                    validationErrors.Add("The connection string contains no 'User ID'");
                if (string.IsNullOrWhiteSpace(csb.Database))
                    validationErrors.Add("The connection string contains no 'Database'");
            }
            catch (Exception ex)
            {
                validationErrors.Add("The connection string is not valid:");
                validationErrors.Add(ex.Message);
            }

            if (!Uri.TryCreate(Environment.GetEnvironmentVariable("INDEXER_RPC_GATEWAY_URL"), UriKind.Absolute, 
                out var rpcGatewayUri))
            {
                validationErrors.Add("Couldn't parse the 'INDEXER_RPC_GATEWAY_URL' environment variable. Expected 'System.Uri'.");
            }
            if (!ushort.TryParse(Environment.GetEnvironmentVariable("INDEXER_WEBSOCKET_PORT"),
                out var websocketPort))
            {
                validationErrors.Add("Couldn't parse the 'INDEXER_WEBSOCKET_PORT' environment variable. Expected 'ushort'.");
            }

            if (validationErrors.Count > 0)
            {
                throw new ArgumentException(string.Join(Environment.NewLine, validationErrors));
            }

            Debug.Assert(rpcGatewayUri != null, nameof(rpcGatewayUri) + " != null");
            
            var indexer = new Indexer(connectionString!, rpcGatewayUri.ToString());
            indexer.NewBlock += (s, e) =>
            {
                Task.Run(() =>
                {
                    try
                    {
                        using var connection = new NpgsqlConnection(connectionString);
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
            
            // TODO: Use cancellation token
            indexer.Run();

            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseUrls(@$"http://localhost:{websocketPort}/");
                    webBuilder.UseStartup<Startup>();
                })
                .Build()
                .Run();
        }
    }
}
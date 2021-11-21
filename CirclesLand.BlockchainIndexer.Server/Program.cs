using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Server
{
    public class Program
    {
        public static string ConnectionString { get; private set; }
        public static string RpcGatewayUrl { get; private set; }
        public static string HostId { get; private set; }

        public static async Task Main(string[] args)
        {
            HostId = Guid.NewGuid().ToString("N");
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

                ConnectionString = connectionString;
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
            else
            {
                RpcGatewayUrl = rpcGatewayUri.ToString();
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

            Debug.Assert(rpcGatewayUri != null, nameof(rpcGatewayUri) + " != null");
            
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseUrls(@$"{websocketUrl}");
                    webBuilder.UseStartup<Startup>();
                })
                .Build();
            
            
            Settings.ConnectionString = connectionString;
            Settings.RpcEndpointUrl = rpcGatewayUri.ToString();
            
            var indexer = new Indexer();
            var cancelIndexerSource = new CancellationTokenSource();
            indexer.Run(cancelIndexerSource.Token).ContinueWith(async t =>
            {
                if (t.Exception != null)
                {
                    Console.WriteLine(t.Exception.Message);
                    Console.WriteLine(t.Exception.StackTrace);
                }
                
                Console.WriteLine("CirclesLand.BlockchainIndexer.Indexer.Run() returned. Stopping the host..");
                await host.StopAsync(TimeSpan.FromSeconds(30));
            });
            
            await host.RunAsync();
        }
    }
}
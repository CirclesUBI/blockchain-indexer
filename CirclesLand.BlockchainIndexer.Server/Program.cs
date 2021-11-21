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
        public static async Task Main(string[] args)
        {
            using var host = Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseUrls(@$"{Settings.WebsocketServerUrl}");
                    webBuilder.UseStartup<Startup>();
                })
                .Build();

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
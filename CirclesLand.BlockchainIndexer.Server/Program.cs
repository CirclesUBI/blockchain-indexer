using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using KestrelWebSocketServer;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;

namespace CirclesLand.BlockchainIndexer.Server
{
    public class Program
    {
        const string RpcUrl = "https://rpc.circles.land";

        public const string ConnectionString =
            @"Server=localhost;Port=5432;Database=circles_land_worker;User ID=postgres;Password=postgres;";
        
        public async static Task Main(string[] args)
        {
            var c = new Indexer(ConnectionString, RpcUrl);
            c.Start();
            
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseUrls(@"http://localhost:8080/");
                    webBuilder.UseStartup<Startup>();
                })
                .Build()
                .Run();
        }

        public static void ReportException(Exception ex, [CallerMemberName] string location = "(Caller name not set)")
        {
            Console.WriteLine($"\n{location}:\n  Exception {ex.GetType().Name}: {ex.Message}");
            if (ex.InnerException != null)
                Console.WriteLine($"  Inner Exception {ex.InnerException.GetType().Name}: {ex.InnerException.Message}");
        }
    }
}
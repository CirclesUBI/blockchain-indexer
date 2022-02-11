using System;
using CirclesLand.BlockchainIndexer.Api;
using CirclesLand.BlockchainIndexer.Server;
using CirclesLand.Host;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace KestrelWebSocketServer
{
    public class Startup
    {

        public void ConfigureServices(IServiceCollection services)
        {
            // register our custom middleware since we use the IMiddleware factory approach
            services.AddTransient<WebsocketService>();
            // services.AddHostedService<IndexerService>();
            // services.AddHostedService<BlockSourceService>();
            // services.AddHostedService<BlockImporterService>();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            // enable websocket support
            app.UseWebSockets(new WebSocketOptions
            {
                KeepAliveInterval = TimeSpan.FromSeconds(120)
            });

            // add our custom middleware to the pipeline
            app.UseMiddleware<WebsocketService>();
        }
    }
}

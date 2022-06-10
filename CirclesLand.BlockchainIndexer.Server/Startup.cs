using CirclesLand.BlockchainIndexer.Api;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Prometheus;

namespace CirclesLand.BlockchainIndexer.Server
{
    public class Startup
    {
        public void ConfigureServices(IServiceCollection services)
        {
            // register our custom middleware since we use the IMiddleware factory approach
            services.AddTransient<TransactionHashBroadcastService>();
            services.AddTransient<HealthService>();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            app.UseRouting();
            app.UseEndpoints(endpoints =>
            {
                // ...
                endpoints.MapMetrics();
            });
            
            // enable websocket support
            app.UseWebSockets(new WebSocketOptions
            {
                // Below values are the default as of Microsoft.AspNetCore.WebSockets, Version=5.0.0.0
                // KeepAliveInterval = TimeSpan.FromSeconds(120),
                // AllowedOrigins = { "*" }
            });
            
            // add our custom middleware to the pipeline
            app.UseMiddleware<HealthService>();
            app.UseMiddleware<TransactionHashBroadcastService>();
        }
    }
}

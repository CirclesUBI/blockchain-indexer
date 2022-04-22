using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CirclesLand.BlockchainIndexer.Util;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CirclesLand.BlockchainIndexer.Api
{
    public class HealthService : IMiddleware
    {
        private static readonly ConcurrentQueue<long> LastBlocks = new();
        
        private static bool _repeatedBlockWarning;
        private static bool _slowImportWarning;
        private static bool _noImport = true;
        private static long _lastKnownBlock = 0;
        private static long _lastImportedBlock = 0;
        private static DateTime _lastCompletedBatch = DateTime.Today;

        public static void ReportStartImportBlock(long block)
        {
            _lastKnownBlock = block > _lastKnownBlock ? block : _lastKnownBlock;
            _repeatedBlockWarning = LastBlocks.Contains(block);

            while (LastBlocks.Count >= 25)
            {
                LastBlocks.TryDequeue(out _);
            }
            LastBlocks.Enqueue(block);
        }
        
        public static void ReportCompleteBatch(long block)
        {
            _noImport = false;
            _lastCompletedBatch = DateTime.Now;
            _lastImportedBlock = block > _lastImportedBlock ? block : _lastImportedBlock;
        }

        private void Tick(object? state)
        {
            if (_repeatedBlockWarning)
            {
                _logger.LogWarning($"Unhealthy: The source yielded repeated blocks: {string.Join(", ", LastBlocks)}");
            }

            _slowImportWarning = _lastCompletedBatch < DateTime.Now - TimeSpan.FromSeconds(30);
            if (_slowImportWarning)
            {
                _logger.LogWarning($"Unhealthy: The last batch was imported {DateTime.Now - _lastCompletedBatch} seconds ago.");
            }
            if (_noImport)
            {
                _logger.LogWarning($"Unhealthy: No import was processed until now.");
            }
        }
        
        private static bool _serverIsRunning = true;
        private static CancellationTokenRegistration _appShutdownHandler;
        private readonly ILogger<WebsocketService> _logger;

        private static Timer? _timer;
        
        public HealthService(IHostApplicationLifetime hostLifetime, ILogger<WebsocketService> logger)
        {
            // gracefully close all websockets during shutdown (only register on first instantiation)
            if (_appShutdownHandler.Token.Equals(CancellationToken.None))
            {
                _appShutdownHandler = hostLifetime.ApplicationStopping.Register(ApplicationShutdownHandler);
            }
            
            _logger = logger;
            _timer = new Timer(Tick, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5));
        }

        public async Task InvokeAsync(HttpContext context, RequestDelegate next)
        {
            try
            {
                if (!_serverIsRunning)
                {
                    context.Response.StatusCode = 503;
                    return;
                }

                if (context.Request.Path != "/health")
                {
                    await next(context);
                    return;
                }

                var issues = new List<string>();
                if (_repeatedBlockWarning)
                {
                    issues.Add("Unhealthy: The source yielded repeated blocks.");
                }
                if (_slowImportWarning)
                {
                    issues.Add("Unhealthy: The import is slow or stale.");
                }
                if (_noImport)
                {
                    issues.Add("Unhealthy: No import was processed until now.");
                }

                var sw = new StreamWriter(context.Response.Body);
                if (issues.Count == 0)
                {
                    await sw.WriteLineAsync("Healthy.");
                    await sw.WriteLineAsync($"Last known block: {_lastKnownBlock}");
                    await sw.WriteLineAsync($"Last imported block: {_lastImportedBlock}");
                    context.Response.StatusCode = 200;
                }
                else
                {
                    await sw.WriteLineAsync("Unhealthy:");
                    issues.ForEach(issue => sw.WriteLine(issue));
                    context.Response.StatusCode = 500;
                }

                await sw.FlushAsync();
            }
            catch (Exception ex)
            {
                // HTTP 500 Internal server error
                context.Response.StatusCode = 500;
                Logger.Log("A connection experienced an error: " + ex.Message);
            }
            finally
            {
                // if this middleware didn't handle the request, pass it on
                if (!context.Response.HasStarted)
                {
                    await next(context);
                }
            }
        }

        // event-handlers are the sole case where async void is valid
        private void ApplicationShutdownHandler()
        {
            _serverIsRunning = false;
            if (_timer != null)
            {
                _timer.Dispose();
                _timer = null;
            }
        }
    }
}
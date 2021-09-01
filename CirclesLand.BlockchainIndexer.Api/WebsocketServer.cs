using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using CirclesLand.BlockchainIndexer.Util;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;

namespace CirclesLand.BlockchainIndexer.Api
{
    public class WebsocketServer : IMiddleware
    {
        public static readonly ConcurrentDictionary<int, ConnectedClient> Clients = new();

        private static int _socketCounter = 0;
        private static bool _serverIsRunning = true;

        private static CancellationTokenRegistration _appShutdownHandler;

        public WebsocketServer(IHostApplicationLifetime hostLifetime)
        {
            // gracefully close all websockets during shutdown (only register on first instantiation)
            if (_appShutdownHandler.Token.Equals(CancellationToken.None))
            {
                _appShutdownHandler = hostLifetime.ApplicationStopping.Register(ApplicationShutdownHandler);
            }
        }

        public static async Task BroadcastMessage(string messageString)
        {
            foreach (var connectedClient in Clients.Values)
            {
                try
                {
                    await connectedClient.SendMessage(messageString);
                }
                catch (Exception e)
                {
                    Logger.LogError(e.Message);
                    Logger.LogError(e.StackTrace);
                }
            }
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

                if (context.WebSockets.IsWebSocketRequest)
                {
                    var socketId = Interlocked.Increment(ref _socketCounter);
                    var socket = await context.WebSockets.AcceptWebSocketAsync();
                    var completion = new TaskCompletionSource<object>();
                    var client = new ConnectedClient(socketId, socket, _appShutdownHandler.Token, completion);

                    if (!Clients.TryAdd(socketId, client))
                    {
                        throw new Exception("Couldn't register the connection at the server.");
                    }

                    await completion.Task;
                }
                else
                {
                    if (context.Request.Headers["Accept"][0].Contains("text/html"))
                    {
                        await context.Response.WriteAsync("CirclesLand.TransactionIndexer WS endpoint.");
                    }
                }
            }
            catch (Exception ex)
            {
                // HTTP 500 Internal server error
                context.Response.StatusCode = 500;
                Logger.LogError(ex.Message);
                Logger.LogError(ex.StackTrace);
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
        private static async void ApplicationShutdownHandler()
        {
            _serverIsRunning = false;
        }
    }
}
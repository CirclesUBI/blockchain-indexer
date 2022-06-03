using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Prometheus;

namespace CirclesLand.BlockchainIndexer.Api
{
    public class ConnectedWebsocketClient : IDisposable
    {   public int SocketId { get; }
        public WebSocket Socket { get; }

        public ConnectedWebsocketClient(
            int socketId, 
            WebSocket socket)
        {
            SocketId = socketId;
            Socket = socket;
        }

        public async Task SendMessage(string messageString)
        {
            var stringBytes = Encoding.UTF8.GetBytes(messageString);
            await Socket.SendAsync(stringBytes, WebSocketMessageType.Text, true, CancellationToken.None);
        }

        public void Dispose()
        {
            Socket?.Dispose();
        }
        
        public async Task ReceiveAsync(HttpContext context, WebSocket socket)
        {
            // TODO: Is this Task.Run really required?
            await Task.Run(async () =>
            {
                var buffer = new byte[8];
                while (!context.RequestAborted.IsCancellationRequested)
                {
                    await socket.ReceiveAsync(buffer, context.RequestAborted);
                    await SendMessage("This service doesn't process any incoming messages.");

                    context.Abort();
                }
            }, context.RequestAborted);
        }
    }
}
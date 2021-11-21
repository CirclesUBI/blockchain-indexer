using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
            Console.WriteLine($"Socket {SocketId} sent: {messageString}");
            var stringBytes = Encoding.UTF8.GetBytes(messageString);
            await Socket.SendAsync(stringBytes, WebSocketMessageType.Text, true, CancellationToken.None);
        }

        public void Dispose()
        {
            Socket?.Dispose();
        }
    }
}
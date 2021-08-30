using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CirclesLand.BlockchainIndexer.Util;

namespace CirclesLand.BlockchainIndexer.Api
{
    public class ConnectedClient
    {
        public int SocketId { get; }
        public WebSocket Socket { get; }
        public TaskCompletionSource<object> TaskCompletion { get; }
        
        public ConnectedClient(
            int socketId, 
            WebSocket socket, 
            CancellationToken cancellationToken,
            TaskCompletionSource<object> taskCompletion)
        {
            SocketId = socketId;
            Socket = socket;
            TaskCompletion = taskCompletion;
            
            Task.Run(() => ReceiveLoop(this, cancellationToken));
        }

        private const int BufferSize = 4096;
        private const int MaxMessageChunkCount = 8;
        
        public static byte[] ConcatByteArrays(params byte[][] arrays)
        {
            var ret = new byte[arrays.Sum(x => x.Length)];
            var offset = 0;
            foreach (var data in arrays)
            {
                Buffer.BlockCopy(data, 0, ret, offset, data.Length);
                offset += data.Length;
            }
            return ret;
        }
        
        private async Task ReceiveLoop(ConnectedClient client, CancellationToken cancellationToken)
        {
            var socket = client.Socket;
            try
            {
                var buffer = WebSocket.CreateServerBuffer(BufferSize);
                while (socket.State != WebSocketState.Closed 
                       && socket.State != WebSocketState.Aborted 
                       && !cancellationToken.IsCancellationRequested)
                {
                    var continueReading = true;
                    var chunks = new List<byte[]>();
                    
                    while(continueReading)
                    {
                        var receiveResult = await client.Socket.ReceiveAsync(buffer, cancellationToken);
                        
                        chunks.Add(buffer[..receiveResult.Count].ToArray());
                        continueReading = !receiveResult.EndOfMessage;

                        if (chunks.Count > MaxMessageChunkCount)
                        {
                            throw new Exception($"The message exceeds {BufferSize * MaxMessageChunkCount} bytes.");
                        }
                    
                        // the client is notifying us that the connection will close; send acknowledgement
                        if (client.Socket.State == WebSocketState.CloseReceived 
                            && receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Acknowledge Close frame", cancellationToken);
                            // the socket state changes to closed at this point
                            return;
                        }
                    }

                    var messageBytes = ConcatByteArrays(chunks.ToArray());
                    var messageString = Encoding.UTF8.GetString(messageBytes);
                    OnReceivedMessage(messageString);

                    /*
                    var request = JsonConvert.DeserializeObject<RpcCall>(messageString);
                    request.SocketId = client.SocketId;

                    var transactionIndexerSystem = TransactionIndexerSystem.System.Value;
                    transactionIndexerSystem.EventStream.Publish(request);
                    */
                }
            }
            catch (OperationCanceledException)
            {
                // normal upon task/token cancellation, disregard
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Socket {client.SocketId}:");
                Logger.LogError(ex.Message);
                Logger.LogError(ex.StackTrace);
            }
            finally
            {
                Console.WriteLine($"Socket {client.SocketId}: Ended processing loop in state {socket.State}");

                // don't leave the socket in any potentially connected state
                if (client.Socket.State != WebSocketState.Closed)
                {
                    client.Socket.Abort();
                }

                // by this point the socket is closed or aborted, the ConnectedClient object is useless
                if (WebsocketServer.Clients.TryRemove(client.SocketId, out _))
                {
                    socket.Dispose();
                }

                // signal to the middleware pipeline that this task has completed
                client.TaskCompletion.SetResult(true);
            }
        }

        private void OnReceivedMessage(string messageString)
        {
            Console.WriteLine($"Socket {SocketId} received: {messageString}");
        }

        public async Task SendMessage(string messageString)
        {
            Console.WriteLine($"Socket {SocketId} sent: {messageString}");
            var stringBytes = Encoding.UTF8.GetBytes(messageString);
            await Socket.SendAsync(stringBytes, WebSocketMessageType.Text, true, CancellationToken.None);
        }
    }
}

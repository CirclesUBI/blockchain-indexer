using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CirclesLand.BlockchainIndexer.Util;
using CirclesLand.Host;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CirclesLand.BlockchainIndexer.Api
{
    public class ConnectedWebsocketClient
    {
        class WebsocketServiceParticipantImpl : ParticipantHostedService
        {
            private long _lastProcessedBlock = 0;
            private ConnectedWebsocketClient _client;

            public WebsocketServiceParticipantImpl(IHostApplicationLifetime applicationLifetime,
                ILogger logger,
                ConnectedWebsocketClient client)
                : base(nameof(ConnectedWebsocketClient), applicationLifetime, logger)
            {
                _client = client;
            }

            public async override Task OnStart(CancellationToken cancellationToken)
            {
                Task.Run(async () =>
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var value = await Participant.WaitForServiceSignalValue(null, "publish",
                            TimeSpan.FromSeconds(1), cancellationToken);

                        await _client.SendMessage(value);
                    }
                });
                
                Logger.LogInformation($"Websocket client {Participant.InstanceId} started.");
            }

            public async override Task OnStop(CancellationToken cancellationToken)
            {
                Logger.LogInformation($"Websocket client {Participant.InstanceId} stopped.");
            }
        }

        private readonly WebsocketServiceParticipantImpl _implementation;
        
        public int SocketId { get; }
        public WebSocket Socket { get; }
        public TaskCompletionSource<object> TaskCompletion { get; }

        private CancellationTokenSource _end = new();
        
        public ConnectedWebsocketClient(
            int socketId, 
            IHostApplicationLifetime applicationLifetime,
            ILogger logger,
            WebSocket socket, 
            CancellationToken cancellationToken,
            TaskCompletionSource<object> taskCompletion)
        {
            var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
                _end.Token,
                cancellationToken);
            
            _implementation = new WebsocketServiceParticipantImpl(applicationLifetime, logger, this);
            _implementation.StartAsync(linkedCancellationTokenSource.Token);
            
            SocketId = socketId;
            Socket = socket;
            TaskCompletion = taskCompletion;
            
            Task.Run(() => ReceiveLoop(this, linkedCancellationTokenSource.Token));
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
        
        private async Task ReceiveLoop(ConnectedWebsocketClient client, CancellationToken cancellationToken)
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
                _end.Cancel();
                
                Console.WriteLine($"Socket {client.SocketId}: Ended processing loop in state {socket.State}");

                // don't leave the socket in any potentially connected state
                if (client.Socket.State != WebSocketState.Closed)
                {
                    client.Socket.Abort();
                }

                // by this point the socket is closed or aborted, the ConnectedClient object is useless
                if (WebsocketService.Clients.TryRemove(client.SocketId, out _))
                {
                    socket.Dispose();
                }

                // signal to the middleware pipeline that this task has completed
                client.TaskCompletion.SetResult(true);
                await _implementation.StopAsync(cancellationToken);
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

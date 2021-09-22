using System;
using System.Threading;
using System.Threading.Tasks;
using CirclesLand.Host;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Web3;

namespace CirclesLand.BlockchainIndexer.Server
{
    public class BlockSourceService : IHostedService
    {
        class BlokSourceServiceImpl : PrimaryWorkerService
        {
            public BlokSourceServiceImpl(IHostApplicationLifetime applicationLifetime,
                ILogger<ParticipantHostedService> logger) 
                : base(nameof(BlockSourceService), applicationLifetime, logger)
            {
            }

            public override string PrimaryLockName => "BlockchainIndexer_Primary";
            public override bool CanEmit => true;
            public override bool CanProcess => false;

            private static long _lastEmittedBlock = 0;

            protected override async Task<string?> Emit(CancellationToken cancellationToken)
            {
                var tcs = new TaskCompletionSource<long?>();

#pragma warning disable 4014
                Task.Run(async () =>
#pragma warning restore 4014
                {
                    try
                    {
                        var timeoutAt = DateTime.UtcNow + TimeSpan.FromSeconds(10);
                        while (DateTime.UtcNow < timeoutAt)
                        {
                            var web3 = new Web3(Program.RpcGatewayUrl);
                            var currentBlock = (await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync()).ToLong();
                            if (currentBlock <= _lastEmittedBlock)
                            {
                                await Task.Delay(1000, cancellationToken);
                            }
                            else
                            {
                                tcs.SetResult(currentBlock);
                                break;
                            }
                        }

                        if (!tcs.Task.IsCompleted)
                        {
                            tcs.SetResult(null);
                        }
                    }
                    catch (Exception ex)
                    {
                        tcs.SetException(ex);
                    }
                }, cancellationToken);

                var block = await tcs.Task;
                if (block == null)
                {
                    return null;
                }
                else
                {
                    _lastEmittedBlock = block.Value;
                }

                return _lastEmittedBlock.ToString();
            }

            protected override Task Process(string payload, CancellationToken cancellationToken)
            {
                throw new NotImplementedException("Cannot Process.");
            }
        }

        private readonly BlokSourceServiceImpl _implementation;
        
        public BlockSourceService(
            IHostApplicationLifetime applicationLifetime,
            ILogger<ParticipantHostedService> logger)
        {
            _implementation = new BlokSourceServiceImpl(applicationLifetime, logger);
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return _implementation.StartAsync(cancellationToken);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return _implementation.StopAsync(cancellationToken);
        }
    }
}
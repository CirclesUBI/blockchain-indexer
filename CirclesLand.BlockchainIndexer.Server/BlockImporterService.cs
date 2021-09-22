using System;
using System.Threading;
using System.Threading.Tasks;
using CirclesLand.Host;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CirclesLand.BlockchainIndexer.Server
{
    public class BlockImporterService : IHostedService
    {
        class BlockImporterServiceImpl : PrimaryWorkerService
        {
            public override string PrimaryLockName => "BlockchainIndexer_Primary";
            public override bool CanEmit => false;
            public override bool CanProcess => true;

            private long _lastProcessedBlock = 0;

            public BlockImporterServiceImpl(IHostApplicationLifetime applicationLifetime,
                ILogger<ParticipantHostedService> logger)
                : base(nameof(BlockImporterService), applicationLifetime, logger)
            {
            }

            protected override async Task<string> Emit(CancellationToken cancellationToken)
            {
                throw new NotImplementedException("Cannot emit");
            }

            protected override async Task Process(string payload, CancellationToken cancellationToken)
            {
                if (!long.TryParse(payload, out var currentBlock))
                {
                    Logger.LogWarning("Cannot parse input as long: " + payload);
                    return;
                }

                if (currentBlock <= _lastProcessedBlock)
                {
                    Logger.LogWarning($"Already processed block {currentBlock}");
                    return;
                }

                // var indexer = new Indexer(Program.ConnectionString, Program.RpcGatewayUrl);
                // indexer.Run();
                
                
                
                _lastProcessedBlock = currentBlock;

                await Participant.SetSignal(true, "publish", payload);
            }
        }

        private readonly BlockImporterServiceImpl _implementation;

        public BlockImporterService(
            IHostApplicationLifetime applicationLifetime,
            ILogger<ParticipantHostedService> logger)
        {
            _implementation = new BlockImporterServiceImpl(applicationLifetime, logger);
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
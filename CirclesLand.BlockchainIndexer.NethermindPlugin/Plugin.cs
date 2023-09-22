using Nethermind.Api;
using Nethermind.Api.Extensions;
using Nethermind.Blockchain;

namespace Circles.Index;

public class CirclesIndex : INethermindPlugin
{
    public string Name => "Circles.Index";
    public string Description => "Circles.Index";
    public string Author => "Daniel Janz (Gnosis Service GmbH)";

    private readonly CancellationTokenSource _cancellationTokenSource = new();

    public async Task Init(INethermindApi nethermindApi)
    {
#pragma warning disable CS4014
        Task.Run(async () =>
#pragma warning restore CS4014
        {
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    (IBlockTree blockTree, long latestBlock) = await GetBlockTree(nethermindApi);
                    CirclesIndexer circlesIndexer = new(nethermindApi, latestBlock);
                    await blockTree.Accept(circlesIndexer, _cancellationTokenSource.Token);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }

        }, _cancellationTokenSource.Token);
    }

    private static async Task<(IBlockTree blockTree, long latestBlock)> GetBlockTree(INethermindApi nethermindApi)
    {
        long? to = nethermindApi.BlockTree?.Head?.Number;

        while (to is null or 0)
        {
            Console.WriteLine("Waiting for the block tree to sync...");
            await Task.Delay(1000);
            to = nethermindApi.BlockTree!.Head!.Number;
        }

        return (nethermindApi.BlockTree!, to.Value);
    }

    #region Default implementation

    public async Task InitNetworkProtocol()
    {
    }

    public async Task InitRpcModules()
    {
    }

    public async ValueTask DisposeAsync()
    {
        _cancellationTokenSource.Cancel();
    }

    #endregion
}

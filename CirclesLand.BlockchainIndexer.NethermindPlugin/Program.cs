using Nethermind.Api;

namespace CirclesLand.BlockchainIndexer.NethermindPlugin;

class Program
{
    static async Task Main(string[] args)
    {
        INethermindApi api = null;
        
        var plugin = new Plugin();
        
        await plugin.Init(api);
        await plugin.InitNetworkProtocol();
        await plugin.InitRpcModules();
    }
}
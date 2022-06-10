using System.Threading.Tasks;
using Akka;
using Akka.Streams.Dsl;
using Nethereum.Hex.HexTypes;

namespace CirclesLand.BlockchainIndexer.Sources
{
    public class SourceFactory
    {
        public Source<HexBigInteger, NotUsed> CreateBulkSource(HexBigInteger from, HexBigInteger to)
        {
            return BulkSource.Create(from.Value, to.Value);
        }

        public Source<HexBigInteger, NotUsed> CreatePollingSource()
        {
            return IntervalSource.Create(Settings.PollingIntervalInMs, Settings.ConnectionString,
                Settings.RpcEndpointUrl);
        }

        public async Task<Source<HexBigInteger,NotUsed>> CreateLiveSource(long lastPersistedBlock)
        {
            return await LiveSource.Create(Settings.ConnectionString, Settings.RpcEndpointUrl, lastPersistedBlock);
        }

        public Source<HexBigInteger,NotUsed> CreateReorgSource()
        {
            return ReorgSource.Create(60000, Settings.ConnectionString, Settings.RpcEndpointUrl);
        }
    }
}
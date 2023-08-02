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
            return IntervalSource.Create(SettingsValues.PollingIntervalInMs, SettingsValues.ConnectionString,
                SettingsValues.RpcEndpointUrl);
        }

        public async Task<Source<HexBigInteger,NotUsed>> CreateLiveSource(long lastPersistedBlock)
        {
            return await LiveSource.Create(SettingsValues.ConnectionString, SettingsValues.RpcEndpointUrl, lastPersistedBlock);
        }

        public Source<HexBigInteger,NotUsed> CreateReorgSource()
        {
            return ReorgSource.Create(60000, SettingsValues.ConnectionString, SettingsValues.RpcEndpointUrl);
        }
    }
}
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
    }
}
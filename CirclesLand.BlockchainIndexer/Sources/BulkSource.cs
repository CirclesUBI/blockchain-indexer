using System.Collections.Generic;
using Akka;
using Akka.Streams.Dsl;
using Nethereum.Hex.HexTypes;
using System.Numerics;
using Prometheus;

namespace CirclesLand.BlockchainIndexer.Sources
{
    public static class SourceMetrics
    {
        
        internal static readonly Counter BlocksEmitted =
            Metrics.CreateCounter("indexer_source_emitted_blocks_total", "How many blocks have been emitted by a source.", "source");
    }
    public static class BulkSource
    {
        public static Source<HexBigInteger, NotUsed> Create(BigInteger from, BigInteger to)
        {
            IEnumerable<HexBigInteger> HexBigIntegerRange()
            {
                for (var i = from; i < to; i++)
                {
                    SourceMetrics.BlocksEmitted.WithLabels("bulk").Inc();
                    
                    yield return new HexBigInteger(i);
                }
            }

            return Source.FromEnumerator(() => HexBigIntegerRange().GetEnumerator());
        }
    }
}
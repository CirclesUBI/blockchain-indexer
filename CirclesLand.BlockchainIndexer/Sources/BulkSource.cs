using System.Collections.Generic;
using Akka;
using Akka.Streams.Dsl;
using Nethereum.Hex.HexTypes;
using System.Numerics;

namespace CirclesLand.BlockchainIndexer.Sources
{
    public static class BulkSource
    {
        public static Source<HexBigInteger, NotUsed> Create(BigInteger from, BigInteger to)
        {
            IEnumerable<HexBigInteger> HexBigIntegerRange()
            {
                for (var i = from; i < to; i++)
                {
                    yield return new HexBigInteger(i);
                }
            }

            return Source.FromEnumerator(() => HexBigIntegerRange().GetEnumerator());
        }
    }
}
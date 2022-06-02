using System;
using System.Collections;
using System.Collections.Generic;
using System.Security.Policy;
using System.Threading.Tasks;
using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Internal;
using CirclesLand.BlockchainIndexer.Util;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;
using Nethereum.Web3;
using Npgsql;
using Prometheus;

namespace CirclesLand.BlockchainIndexer.Sources
{
    public static class GapSource
    {

        private static Queue<HexBigInteger> _missingBlocks = new();
        private static bool first = true;

        public static Source<HexBigInteger, NotUsed> Create(int intervalInMs, string connectionString, bool finish = false)
        {
            var fin = finish;
            return Source.UnfoldAsync(new HexBigInteger(0), async (lastGapBlock) =>
            {
                await using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                while (!fin)
                {
                    try
                    {
                        if (_missingBlocks.Count == 0)
                        {
                            if (!first && finish)
                            {
                                fin = true;
                                break;
                            }
                            Logger.Log($"Checking for gaps ..");
                            var missingBlocks = FindMissingBlocks(connection);
                            missingBlocks.ForEach(o => _missingBlocks.Enqueue(o));
                            if (!first)
                            {
                                await Task.Delay(intervalInMs);
                            }
                            first = false;
                        }
                        else
                        {
                            var a = _missingBlocks.Dequeue();
                            Logger.Log($"Emitting missing block: {a.ToLong()}");
                            SourceMetrics.BlocksEmitted.WithLabels("gap").Inc();
                            
                            return new Option<(HexBigInteger, HexBigInteger)>((a, a));
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex.Message);
                        if (ex.StackTrace != null) Logger.LogError(ex.StackTrace);

                        throw;
                    }
                }

                return new Option<(HexBigInteger, HexBigInteger)>();
            });
        }

        public static IEnumerable<HexBigInteger> FindMissingBlocks(NpgsqlConnection connection)
        {
            using var cmd =
                new NpgsqlCommand(
                    $@"select (number + 1)::bigint as gap_start,
                               (next_nr - 1)::bigint as gap_end
                        from (
                                 select number,
                                        lead(number) over (order by number) as next_nr
                                 from block
                             ) nr
                        where number + 1 <> next_nr
                          and number >= 12529456;",
                    connection);
            using var reader = cmd.ExecuteReader();

            var row = new object[reader.FieldCount];

            while (reader.Read())
            {
                reader.GetValues(row);
                for (var start = (long) row[0]; start <= (long) row[1]; start++)
                {
                    yield return new HexBigInteger(start);
                }
            }
        }
    }
}
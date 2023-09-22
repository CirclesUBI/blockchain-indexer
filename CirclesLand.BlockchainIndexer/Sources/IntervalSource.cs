using System;
using System.Threading.Tasks;
using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using CirclesLand.BlockchainIndexer.Util;
using Dapper;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.Web3;
using Npgsql;
using Prometheus;

namespace CirclesLand.BlockchainIndexer.Sources
{
    /// <summary>
    /// Checks the chain for new blocks at the specified interval.
    /// If blocks have been missed between two executions then all
    /// blocks that preceded the current one will be emitted as well (catch up).
    /// </summary>
    public static class IntervalSource
    {
        public static Source<HexBigInteger, NotUsed> Create(int intervalInMs, string connectionString, string rpcUrl)
        {
            return Source.UnfoldAsync(new HexBigInteger(0), async lastBlock =>
            {
                await using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var web3 = new Web3(rpcUrl);

                while (true)
                {
                    try
                    {
                        // Determine if we need to catch up (database old)
                        var currentBlock = await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync();
                        var lastIndexedBlock =
                            connection.QuerySingleOrDefault<long?>("select max(number) from block") ?? 0;

                        if (lastBlock.Value == 0)
                        {
                            lastBlock = new HexBigInteger(lastIndexedBlock == 0 ? SettingsValues.StartFromBlock : lastIndexedBlock);
                        }

                        if (currentBlock.ToLong() > lastIndexedBlock && currentBlock.Value > lastBlock.Value)
                        {
                            var nextBlockToIndex = lastBlock.Value + 1;
                            Console.WriteLine($"Catching up block: {nextBlockToIndex}");

                            return Option<(HexBigInteger, HexBigInteger)>.Create((new HexBigInteger(nextBlockToIndex),
                                new HexBigInteger(nextBlockToIndex)));
                        }

                        await Task.Delay(intervalInMs);

                        // At this point we wait for a new block
                        currentBlock = await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync();
                        if (currentBlock == lastBlock)
                        {
                            continue;
                        }

                        Console.WriteLine($"Got new block: {currentBlock}");
                        SourceMetrics.BlocksEmitted.WithLabels("interval").Inc();
                        
                        return Option<(HexBigInteger, HexBigInteger)>.Create((currentBlock, currentBlock));
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex.Message);
                        if (ex.StackTrace != null) Logger.LogError(ex.StackTrace);

                        throw;
                    }
                }
            });
        }
    }
}
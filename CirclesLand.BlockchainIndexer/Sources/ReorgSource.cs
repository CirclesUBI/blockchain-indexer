using System;
using System.Threading.Tasks;
using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using CirclesLand.BlockchainIndexer.Util;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;
using Nethereum.Web3;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Sources
{
    public static class ReorgSource
    {
        public static readonly ConcurrentSet<long> BlockReorgsSharedState = new ();

        public static Source<HexBigInteger, NotUsed> Create(int intervalInMs, string connectionString, string rpcUrl)
        {
            return Source.UnfoldAsync(new HexBigInteger(0), async _ =>
            {
                await using var connection = new NpgsqlConnection(connectionString);
                connection.Open();

                var web3 = new Web3(rpcUrl);
                
                while (true)
                {
                    try
                    {
                        await Task.Delay(intervalInMs);

                        Logger.Log($"Checking for reorgs ..");
                        
                        var oldestReorgBlock = await CheckForReorgsInLastBlocks(connection, web3);
                        if (oldestReorgBlock < long.MaxValue)
                        {   
                            SourceMetrics.BlocksEmitted.WithLabels("reorg").Inc();
                            
                            return Option<(HexBigInteger, HexBigInteger)>.Create((new HexBigInteger(oldestReorgBlock),
                                new HexBigInteger(oldestReorgBlock)));;                            
                        }

                        Logger.Log($"No reorgs.");
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

        public static async Task<long> CheckForReorgsInLastBlocks(NpgsqlConnection connection, Web3 web3, long? fromBlock = null, long blockCount = 18)
        {
            await using var cmd =
                new NpgsqlCommand(
                    $"select number, hash from block where number >= {(fromBlock.HasValue ? fromBlock : "(select max(number) from block)")} - {blockCount} order by number desc",
                    connection);
            await using var reader = cmd.ExecuteReader();

            var row = new object[reader.FieldCount];
            var oldestReorgBlock = long.MaxValue;

            while (reader.Read())
            {
                reader.GetValues(row);

                var block = await web3.Eth.Blocks.GetBlockWithTransactionsHashesByNumber.SendRequestAsync(
                    new BlockParameter(Convert.ToUInt64(row[0])));

                if (block.BlockHash != row[1].ToString())
                {
                    oldestReorgBlock = block.Number.ToLong();
                }
            }

            if (oldestReorgBlock < long.MaxValue)
            {
                Logger.Log($"Reorg detected at block height: {oldestReorgBlock}.");
                BlockReorgsSharedState.TryAdd(oldestReorgBlock);
            }

            return oldestReorgBlock;
        }
    }
}
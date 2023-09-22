using System;
using Dapper;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.RPC.Eth.DTOs;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence
{
    public class BlockTracker
    {
        public static int UnflushedEmptyBlocks = 0;
        
        public static long GetLastValidBlock(NpgsqlConnection connection, long defaultBlockNo)
        {
            var lastKnownBlock = connection.QuerySingleOrDefault<long?>(
                @"with a as (
                        select distinct block_no
                        from requested_blocks
                        order by block_no
                    ), b as (
                        select distinct number
                        from block
                        order by number
                    ), c as (
                        select a.block_no as requested, b.number as actual
                        from a
                                 left join b on a.block_no = b.number
                        where a.block_no >= 12529456
                        order by a.block_no
                    )
                    select coalesce(min(c.requested), (select max(number) from block)) - 1 as last_correctly_imported_block
                    from c
                    where actual is null;") ?? defaultBlockNo;

            return lastKnownBlock;
        }

        public static void AddRequested(NpgsqlConnection writerConnection, long number)
        {
            writerConnection.Execute($@"
                                    insert into requested_blocks (block_no)
                                    values (@number) on conflict do nothing;",
                new
                {
                    number
                });
        }

        public static void InsertEmptyBlock(NpgsqlConnection writerConnection, long blockTimestamp, long blockNumber, string blockHash)
        {
            var blockTimestampDateTime =
                DateTimeOffset.FromUnixTimeSeconds(blockTimestamp).UtcDateTime;

            writerConnection.Execute($@"
                                    insert into {TransactionsWriter.blockTableName} (number, hash, timestamp, total_transaction_count)
                                    values (@number, @hash, @timestamp, 0);",
                new
                {
                    number = blockNumber,
                    hash = blockHash,
                    timestamp = blockTimestampDateTime
                });

            UnflushedEmptyBlocks++;
        }
    }
}
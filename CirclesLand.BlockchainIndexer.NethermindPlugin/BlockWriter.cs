using Npgsql;
using NpgsqlTypes;

namespace CirclesLand.BlockchainIndexer.NethermindPlugin
{
    public class BlockWriter
    {
        public static int WriteBlocks(NpgsqlConnection writerConnection,
            string blockTableName,
            IEnumerable<(long BlockNumber, DateTime BlockTimestamp, string Hash, int TotalTransactionCount)> blocks)
        {
            using var writer = writerConnection.BeginBinaryImport(
                @$"COPY {blockTableName} (
                                         number
                                        ,hash
                                        ,timestamp
                                        ,total_transaction_count
                                        ,selected_at
                                        ,imported_at
                                    ) FROM STDIN (FORMAT BINARY)");

            var rowCount = 0;
            foreach (var d in blocks)
            {
                writer.StartRow();
                writer.Write(d.BlockNumber, NpgsqlDbType.Bigint);
                writer.Write(d.Hash, NpgsqlDbType.Text);
                writer.Write(d.BlockTimestamp, NpgsqlDbType.Timestamp);
                writer.Write(d.TotalTransactionCount, NpgsqlDbType.Integer);
                writer.Write(DBNull.Value, NpgsqlDbType.Timestamp);
                writer.Write(DBNull.Value, NpgsqlDbType.Timestamp);

                rowCount++;
            }

            writer.Complete();
            
            return rowCount;
        }
    }
}
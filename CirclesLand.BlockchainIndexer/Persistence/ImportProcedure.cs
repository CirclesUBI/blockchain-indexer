using System;
using System.Data;
using Dapper;
using Npgsql;
using Prometheus;

namespace CirclesLand.BlockchainIndexer.Persistence
{
    public class ImportProcedure
    {
        private static readonly Gauge DatabaseSize = 
            Metrics.CreateGauge("indexer_db_size", "The size of the indexer database in bytes.");
        
        private static readonly Gauge TotalRowCount = 
            Metrics.CreateGauge("indexer_db_total_row_count", "The total number of rows in each table of the database.", "table");
        
        public static void ImportFromStaging(NpgsqlConnection connection, int timeout)
        {
            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);

            try
            {
                connection.Execute("call import_from_staging_2();", null, transaction, timeout);
                transaction.Commit();

                var databaseSize = new NpgsqlCommand("SELECT pg_database_size('indexer');", connection).ExecuteScalar();
                DatabaseSize.Set((long)(databaseSize ?? -1));

                ReportRowCounts(connection, "block");
                ReportRowCounts(connection, "crc_hub_transfer_2");
                ReportRowCounts(connection, "crc_organisation_signup_2");
                ReportRowCounts(connection, "crc_signup_2");
                ReportRowCounts(connection, "crc_trust_2");
                ReportRowCounts(connection, "erc20_transfer_2");
                ReportRowCounts(connection, "eth_transfer_2");
                ReportRowCounts(connection, "gnosis_safe_eth_transfer_2");
                ReportRowCounts(connection, "requested_blocks");
                ReportRowCounts(connection, "transaction_2");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                transaction.Rollback();
                throw;
            }
        }

        private static void ReportRowCounts(NpgsqlConnection connection, string ofTable)
        {
            var rowCountCmd = new NpgsqlCommand(@"
                SELECT (reltuples / (case when relpages = 0 then 1 else relpages end) * (pg_relation_size(oid) / 8192))::bigint
                FROM   pg_class
                WHERE  oid = $1::regclass;"
                , connection);

            rowCountCmd.Parameters.AddWithValue(ofTable);
            var scalarResult = rowCountCmd.ExecuteScalar();
            
            TotalRowCount.WithLabels(ofTable).Set((long)(scalarResult ?? -1));
        }
    }
}
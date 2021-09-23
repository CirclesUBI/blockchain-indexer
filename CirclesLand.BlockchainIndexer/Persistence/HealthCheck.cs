using System;
using Dapper;
using Newtonsoft.Json;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence
{
    public class HealthCheck
    {
        public static bool IsHealthy(
            NpgsqlConnection connection, 
            int maxHealthyBacklogSize,
            int timeoutInSeconds)
        {
            var healthCheckSql =
                @$"with max_imported as (
                        select max(number) as number from block
                    ), max_staging as (
                        select max(number) as number from _block_staging
                    ), min_missing as (
                        select min(block_no) -1 missing_block_begin
                        from requested_blocks rb
                        left join block b on rb.block_no = b.number and b.number < (select number from max_imported)
                        where b.number is null
                    ), c as (
                        select (select number from max_staging) - (select number from max_imported) as staging_distance
                             , (select number from max_imported) - missing_block_begin              as imported_distance
                        from min_missing
                    )
                    select *
                    from c
                    where c.imported_distance >= {maxHealthyBacklogSize}
                       or c.staging_distance >= {maxHealthyBacklogSize};";

            var healthCheckResult = connection.QueryFirstOrDefault(
                healthCheckSql,
                null,
                null, 
                timeoutInSeconds);

            return healthCheckResult == null;
        }
    }
}
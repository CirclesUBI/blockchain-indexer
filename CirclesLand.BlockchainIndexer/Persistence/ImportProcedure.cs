using System;
using System.Data;
using Dapper;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence
{
    public class ImportProcedure
    {
        public static void ImportFromStaging(NpgsqlConnection connection)
        {
            using var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

            try
            {
                connection.Execute("call import_from_staging_2();", null, transaction, 120);
                transaction.Commit();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                transaction.Rollback();
                throw;
            }
        }
    }
}
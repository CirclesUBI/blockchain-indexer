/*
using System.Linq;
using Nethereum.RPC.Eth.DTOs;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence
{
    public class LogTopicWriter
    {
        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlTransaction _dbTransaction;
        
        public LogTopicWriter(NpgsqlConnection connection, NpgsqlTransaction? dbTransaction)
        {
            _connection = connection;
            _dbTransaction = dbTransaction;
        }
        
        public void Write(Transaction transaction)
        {
            var allLogTopics = transaction.Logs.SelectMany(o => o.Topics);
            foreach (var logTopic in allLogTopics)
            {
                "insert into \"LogTopics\" (\"Topic\") values (@1) on conflict do nothing ;"
                    .ToCommand(_connection, _dbTransaction, logTopic)
                    .ExecuteNonQuery();
            }
            
            if (_dbTransaction != null)
            {
                _dbTransaction.Commit();
            }
        }
    }
}
*/
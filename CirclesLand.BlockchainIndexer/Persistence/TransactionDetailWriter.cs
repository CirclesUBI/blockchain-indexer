using System;
using System.Collections.Generic;
using CirclesLand.BlockchainIndexer.Persistence.DetailWriters;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Npgsql;

namespace CirclesLand.BlockchainIndexer.Persistence
{
    public class TransactionDetailWriter
    {
        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlTransaction? _dbTransaction;

        public TransactionDetailWriter(NpgsqlConnection connection, NpgsqlTransaction? dbTransaction)
        {
            _connection = connection;
            _dbTransaction = dbTransaction;
        }

        public void Write( 
            string hash,
            int index,
            DateTime timestamp,
            long block_number,
            IEnumerable<IDetail> details)
        {
            foreach (var detail in details)
            {
                switch (detail)
                {
                    case CrcTrust trust:
                        CrcTrustWriter.Insert(_connection, _dbTransaction, hash, index, timestamp, block_number, trust);
                        break;
                    case CrcHubTransfer crcTransfer:
                        CrcTransferWriter.Insert(_connection, _dbTransaction, hash, index, timestamp, block_number, crcTransfer);
                        break;
                    case Erc20Transfer erc20Transfer:
                        Erc20TransferWriter.Insert(_connection, _dbTransaction, hash, index, timestamp, block_number, erc20Transfer);
                        break;
                    case GnosisSafeEthTransfer gnosisSafeEthTransfer:
                        GnosisSafeEthTransferWriter.Insert(_connection, _dbTransaction, hash, index, timestamp, block_number, gnosisSafeEthTransfer);
                        break;
                    case EthTransfer ethTransfer:
                        EthTransferWriter.Insert(_connection, _dbTransaction, hash, index, timestamp, block_number, ethTransfer);
                        break;
                    case CrcSignup crcSignup:
                        CrcSignupWriter.Insert(_connection, _dbTransaction, hash, index, timestamp, block_number, crcSignup);
                        break;
                    case CrcOrganisationSignup crcOrganisationSignup:
                        CrcOrganisationSignupWriter.Insert(_connection, _dbTransaction, hash, index, timestamp, block_number, crcOrganisationSignup);
                        break;
                }
            }
        }
    }
}
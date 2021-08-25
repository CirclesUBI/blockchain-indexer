using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
        
        public ImmutableArray<Tuple<string, long>> Write(
            long transactionId, 
            IEnumerable<IDetail> details)
        {
            var detailIds = new List<Tuple<string, long>>();

            foreach (var detail in details)
            {
                switch (detail)
                {
                    case CrcTrust trust:
                        detailIds.Add(Tuple.Create("CrcTrust", CrcTrustWriter.Insert(_connection, _dbTransaction, transactionId, trust)));
                        break;
                    case CrcHubTransfer crcTransfer:
                        detailIds.Add(Tuple.Create("CrcHubTransfer", CrcTransferWriter.Insert(_connection, _dbTransaction, transactionId, crcTransfer)));
                        break;
                    case Erc20Transfer erc20Transfer:
                        detailIds.Add(Tuple.Create("Erc20Transfer", Erc20TransferWriter.Insert(_connection, _dbTransaction, transactionId, erc20Transfer)));
                        break;
                    case GnosisSafeEthTransfer gnosisSafeEthTransfer:
                        detailIds.Add(Tuple.Create("GnosisSafeEthTransfer", GnosisSafeEthTransferWriter.Insert(_connection, _dbTransaction, transactionId, gnosisSafeEthTransfer)));
                        break;
                    case EthTransfer ethTransfer:
                        detailIds.Add(Tuple.Create("EthTransfer", EthTransferWriter.Insert(_connection, _dbTransaction, transactionId, ethTransfer)));
                        break;
                    /*
                    case TokenMinting tokenMinting:
                        detailIds.Add(Tuple.Create("TokenMinting", TokenMintingWriter.Insert(_connection, _dbTransaction, transactionId, tokenMinting)));
                        break;
                    case TransactionMessage transactionMessage:
                        detailIds.Add(Tuple.Create("TransactionMessage", TransactionMessageWriter.Insert(_connection, _dbTransaction, transactionId, transactionMessage)));
                        break;
                    case InvitationEoa invitationEoa:
                        detailIds.Add(Tuple.Create("InvitationEoa", InvitationEoaWriter.Insert(_connection, _dbTransaction, invitationEoa)));
                        break;
                    case InvitationRedemption invitationRedemption:
                        detailIds.Add(Tuple.Create("InvitationRedemption", InvitationRedemptionWriter.Insert(_connection, _dbTransaction, transactionId, invitationRedemption)));
                        break;
                    */
                    case CrcSignup crcSignup:
                        detailIds.Add(Tuple.Create("CrcSignup", CrcSignupWriter.Insert(_connection, _dbTransaction, transactionId, crcSignup)));
                        break;
                    case CrcOrganisationSignup crcOrganisationSignup:
                        detailIds.Add(Tuple.Create("CrcOrganisationSignup", CrcOrganisationSignupWriter.Insert(_connection, _dbTransaction, transactionId, crcOrganisationSignup)));
                        break;
                }
            }
            
            return detailIds.ToImmutableArray();
        }
    }
}
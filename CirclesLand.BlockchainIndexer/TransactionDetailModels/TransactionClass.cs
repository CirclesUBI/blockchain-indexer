using System;

namespace CirclesLand.BlockchainIndexer.TransactionDetailModels
{
    [Flags]
    public enum TransactionClass
    {
        Unknown = 0,
        Erc20Transfer = 1,
        CrcSignup = 2,
        CrcOrganisationSignup = 4,
        CrcHubTransfer = 8,
        CrcTrust = 16,
        EoaEthTransfer = 32,
        SafeEthTransfer = 64,
        ContractCreation = 128,
    }
}
using System;

namespace CirclesLand.BlockchainIndexer.TransactionDetailModels
{
    [Flags]
    public enum TransactionClass
    {
        Unknown = 0,
        EoaEthTransfer = 1,
        Erc20Transfer = 2,
        CrcTransfer = 4,
        CrcSignup = 8,
        CrcOrganisationSignup = 16,
        TokenMinting = 32,
        CrcTrust = 64,
        InvitationCreation = 128,
        InvitationRedemption = 256,
        InitialSafeFunding = 512,
        GnosisSafeEthTransfer = 1024,
        ContractCreation = 2048,
        SafeCreation = 4096
    }
}
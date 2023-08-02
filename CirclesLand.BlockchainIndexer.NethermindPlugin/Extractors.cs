using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethermind.Core;

namespace CirclesLand.BlockchainIndexer.NethermindPlugin;

public class Extractors
{
    public static void ExtractCrcTrust(LogEntry log, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (!TransactionClassifier.IsCrcTrust(log, out var canSendTo, out var user, out var limit))
            {
                return;
            }
            
            Console.WriteLine($"* CRC Trust: {user} -> {canSendTo} ({limit})");
            events.Add((TransactionClass.CrcTrust, new CrcTrust
            {
                Address = user,
                CanSendTo = canSendTo,
                Limit = limit.Value.ToInt64(null)
            }, null));
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    public static void ExtractCrcSignup(TxReceipt receipt, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (!TransactionClassifier.IsCrcSignup(receipt, out var userAddress, out var tokenAddress))
            {
                return;
            }
            
            Console.WriteLine($"* CRC Signup: {userAddress} -> {tokenAddress}");
            events.Add((TransactionClass.CrcSignup, new CrcSignup
            {
                User = userAddress,
                Token = tokenAddress
            }, null));
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    public static void ExtractCrcHubTransfer(TxReceipt receipt, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (!TransactionClassifier.IsCrcHubTransfer(receipt, out var from, out var to, out var amount))
            {
                return;
            }
            
            Console.WriteLine($"* CRC Hub Transfer: {from} -> {to} ({amount})");
            events.Add((TransactionClass.CrcHubTransfer, new CrcHubTransfer
            {
                From = from,
                To = to,
                Value = amount?.ToString()
            }, null));
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    public static void ExtractCrcOrganisationSignup(LogEntry log, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (!TransactionClassifier.IsCrcOrganisationSignup(log, out var organisationAddress))
            {
                return;
            }
            
            Console.WriteLine($"* CRC Organisation Signup: {organisationAddress}");
            events.Add((TransactionClass.CrcOrganisationSignup, new CrcOrganisationSignup
            {
                Organization = organisationAddress
            }, null));
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    public static void ExtractErc20Transfer(LogEntry log, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (!TransactionClassifier.IsErc20Transfer(log, out var tokenAddress, out var from, out var to, out var erc20Amount))
            {
                return;
            }
            
            Console.WriteLine($"* ERC20 Transfer: {from} -> {to} ({erc20Amount})");
            events.Add((TransactionClass.Erc20Transfer, new Erc20Transfer
            {
                Token = tokenAddress,
                From = from,
                To = to,
                Value = erc20Amount?.ToString()
            }, null));
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }

    public static void ExtractEoaEthTransfer(Transaction transaction, TxReceipt receipt, List<(TransactionClass, IDetail?, Exception?)> events)
    {
        try
        {
            if (TransactionClassifier.IsEoaEthTransfer(transaction, receipt, out var from, out var to,
                    out var ethAmount))
            {
                Console.WriteLine($"* EOA ETH Transfer: {from} -> {to} ({ethAmount})");
                events.Add((TransactionClass.EoaEthTransfer, new EthTransfer
                {
                    From = from,
                    To = to,
                    Value = ethAmount?.ToString()
                }, null));
            }
        }
        catch (Exception e)
        {
            events.Add((TransactionClass.Unknown, null, e));
        }
    }
}
using System.Collections.Generic;
using System.Diagnostics;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.ABI.FunctionEncoding;
using Nethereum.ABI.Model;
using Nethereum.RPC.Eth.DTOs;

namespace CirclesLand.BlockchainIndexer.DetailExtractors
{
    public static class GnosisSafeEthTransferDetailExtractor
    {
        public static IEnumerable<IDetail> Extract(Transaction transactionData, TransactionReceipt receipt)
        {
            // execTransaction(
            //      address to,
            //      uint256 value,
            //      bytes data,
            //      uint8 operation,
            //      uint256 safeTxGas,
            //      uint256 baseGas,
            //      uint256 gasPrice,
            //      address gasToken,
            //      address refundReceiver,
            //      bytes signatures)
            var decoded = new FunctionCallDecoder()
                .DecodeFunctionInput(
                    "0x6a761202", 
                    transactionData.Input, 
                    new Parameter("address", 0), 
                    new Parameter("uint256", 1), 
                    new Parameter("bytes", 2), 
                    new Parameter("uint8", 3), 
                    new Parameter("uint256", 4), 
                    new Parameter("uint256", 5), 
                    new Parameter("uint256", 6), 
                    new Parameter("address", 7), 
                    new Parameter("address", 8), 
                    new Parameter("bytes", 9));

            var from = transactionData.To;
            var to = decoded[0].Result.ToString();
            var valueBigInteger = decoded[1].Result.ToString();
            Debug.Assert(valueBigInteger != null);

            yield return new GnosisSafeEthTransfer
            {
                From = from,
                To = to,
                Value = valueBigInteger
            };
        }
    }
}
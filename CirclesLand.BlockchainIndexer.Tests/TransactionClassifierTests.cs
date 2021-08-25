using System.Linq;
using System.Threading.Tasks;
using CirclesLand.BlockchainIndexer.DetailExtractors;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Nethereum.ABI.FunctionEncoding;
using Nethereum.ABI.Model;
using Nethereum.Web3;
using NUnit.Framework;

namespace CirclesLand.BlockchainIndexer.Tests
{
    public class TransactionClassifierTests
    {
        const string RpcUrl = "https://rpc.circles.land";
        private static readonly Web3 _web3 = new(RpcUrl);
        
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public async Task TestErc20Transfer()
        {
            var receipt = await _web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(
                "0x063662962348cd891401653f3670d5ea59cb92de2a2f4a981f8fce33e7c94d22");

            var transfers = receipt.Logs.Where(o => TransactionClassifier.IsErc20Transfer(o, out _, out _, out _, out _));
            Assert.IsTrue(transfers.Count() == 2);
        }

        [Test]
        public async Task TestCrcSignup()
        {
            var receipt = await _web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(
                "0xe26765e8c4ebe2388a39a09ec0bd10f256aaca1c8734b217f8047e387b40dbd6");

            var signup = TransactionClassifier.IsCrcSignup(receipt, out var addres, out var tokenAddress);
            Assert.IsTrue(signup);
        }

        [Test]
        public async Task TestCrcOrganisationSignup()
        {
            var receipt = await _web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(
                "0x6ef6b71484a8af7b71622d886bd7be9d20dc157cac3b6b69f784c9d29b080b33");

            var orgaSignups = receipt.Logs.Where(o => TransactionClassifier.IsCrcOrganisationSignup(o, out _));
            Assert.IsTrue(orgaSignups.Count() == 1);
        }

        [Test]
        public async Task TestCrcHubTransfer()
        {
            var receipt = await _web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(
                "0x049e87a478dcdee9420a85d1e11068bf947c159e2e9760e7e616b045df0adb74");

            var hubTransfer = TransactionClassifier.IsCrcHubTransfer(receipt, out _, out _, out _);
            Assert.IsTrue(hubTransfer);
        }

        [Test]
        public async Task TestCrcTrust()
        {
            var receipt = await _web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(
                "0xeef84f999624e38d47663a8764f4ae15260b21f4b4e7f2c1c7f39f886ee7ad6a");

            var trust = receipt.Logs.Where(o => TransactionClassifier.IsCrcTrust(o, out _, out _, out _));
            Assert.IsTrue(trust.Count() == 1);
        }

        [Test]
        public async Task TestEoaEthTransfer()
        {
            var transaction = await _web3.Eth.Transactions.GetTransactionByHash.SendRequestAsync(
                "0x12731b64b1b713f4057646aecacb4c7740720ec70caf9da78b8ba56a932290dd");
            var receipt = await _web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(
                "0x12731b64b1b713f4057646aecacb4c7740720ec70caf9da78b8ba56a932290dd");

            var isEthTransfer = TransactionClassifier.IsEoaEthTransfer(
                transaction, receipt, out _, out _, out _);
            
            Assert.IsTrue(isEthTransfer);
        }

        [Test]
        public async Task TestSafeEthTransfer()
        {
            var transaction = await _web3.Eth.Transactions.GetTransactionByHash.SendRequestAsync(
                "0x5ff9093d7b0b95976261c7562ad5c32e32684dd4f20e23178f2ed5c5ac14380c");
            var receipt = await _web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(
                "0x5ff9093d7b0b95976261c7562ad5c32e32684dd4f20e23178f2ed5c5ac14380c");

            var isSafeEthTransfer = TransactionClassifier.IsSafeEthTransfer(
                transaction, 
                receipt,
                out var initiator,
                out var from,
                out var to,
                out var value);
            
            Assert.IsTrue(isSafeEthTransfer);
        }
    }
}
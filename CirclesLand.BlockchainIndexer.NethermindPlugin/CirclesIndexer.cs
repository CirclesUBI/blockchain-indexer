using System.Diagnostics;
using System.Globalization;
using Nethermind.Api;
using Nethermind.Blockchain.Visitors;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Extensions;
using Nethermind.Int256;

namespace Circles.Index
{
    public class CirclesIndexer : IBlockTreeVisitor
    {
        public bool PreventsAcceptingNewBlocks => true;
        public bool CalculateTotalDifficultyIfMissing => false;
        public long StartLevelInclusive { get; private set; }
        public long EndLevelExclusive { get; private set; }

        private readonly SqlitePersistence _persistence;

        private readonly Stopwatch _stopwatch = new();
        private long _blocksProcessed;

        private static readonly string _addressEmptyBytesPrefix = "0x000000000000000000000000";
        private static readonly Address _circlesHubAddress = new("0x29b9a7fBb8995b2423a71cC17cf9810798F6C543");

        private static readonly Keccak _crcHubTransferEventTopic =
            new Keccak("0x8451019aab65b4193860ef723cb0d56b475a26a72b7bfc55c1dbd6121015285a");

        private static readonly Keccak _crcTrustEventTopic =
            new("0xe60c754dd8ab0b1b5fccba257d6ebcd7d09e360ab7dd7a6e58198ca1f57cdcec");

        private static readonly Keccak _transferEventTopic =
            new("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

        private static readonly Keccak _crcSignupEventTopic =
            new("0x358ba8f768af134eb5af120e9a61dc1ef29b29f597f047b555fc3675064a0342");

        private static readonly Keccak _crcOrganisationSignupEventTopic =
            new("0xb0b94cff8b84fc67513b977d68a5cdd67550bd9b8d99a34b570e3367b7843786");

        private static readonly Keccak _erc20TransferTopic =
            new("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

        private readonly INethermindApi _nethermindApi;

        public CirclesIndexer(INethermindApi nethermindApi, long latestBlock)
        {
            _nethermindApi = nethermindApi;

            _persistence = new SqlitePersistence("circles-index.sqlite");
            _persistence.Initialize();

            StartLevelInclusive = _persistence.GetLastRelevantBlock() + 1;
            if (StartLevelInclusive < 12529458)
                StartLevelInclusive = 12529458;

            EndLevelExclusive = latestBlock + 1;

            _stopwatch.Start();
        }

        public async Task<LevelVisitOutcome> VisitLevelStart(ChainLevelInfo chainLevelInfo, long levelNumber,
            CancellationToken cancellationToken)
        {
            // Logic to execute at the start of visiting a new chain level
            _blocksProcessed++;

            if (_blocksProcessed % 5000 == 0)
            {
                double blocksPerSecond = _blocksProcessed / _stopwatch.Elapsed.TotalSeconds;
                Console.WriteLine(
                    $"Processed {_blocksProcessed} blocks in {_stopwatch.Elapsed.TotalSeconds} seconds. Current speed: {blocksPerSecond} blocks/sec.");
            }

            return LevelVisitOutcome.None;
        }

        public async Task<bool> VisitMissing(Keccak hash, CancellationToken cancellationToken)
        {
            // Logic to execute when a block is missing
            return true;
        }

        public async Task<HeaderVisitOutcome> VisitHeader(BlockHeader header, CancellationToken cancellationToken)
        {
            // Logic to execute when visiting a block header
            return HeaderVisitOutcome.None;
        }

        public async Task<BlockVisitOutcome> VisitBlock(Block block, CancellationToken cancellationToken)
        {
            if (block.Hash == null)
            {
                throw new Exception("Block hash is null");
            }

            if (_nethermindApi.ReceiptFinder == null)
            {
                throw new Exception("ReceiptFinder is null");
            }

            bool blockIsRelevant = false;

            TxReceipt[] receipts = _nethermindApi.ReceiptFinder.Get(block);
            foreach (TxReceipt txReceipt in receipts)
            {
                if (txReceipt.Logs == null)
                    continue;

                foreach (LogEntry log in txReceipt.Logs)
                {
                    if (log.Topics.Length == 0)
                        continue;

                    Keccak topic = log.Topics[0];

                    if (topic == _erc20TransferTopic && _persistence.IsCirclesToken(log.LoggersAddress))
                    {
                        string tokenAddress = log.LoggersAddress.ToString(true, false);
                        string from = log.Topics[1].ToString().Replace(_addressEmptyBytesPrefix, "0x");
                        string to = log.Topics[2].ToString().Replace(_addressEmptyBytesPrefix, "0x");
                        UInt256 value = new(log.Data, true);

                        _persistence.AddCirclesTransfer(block.Number, txReceipt.TxHash!.ToString(), tokenAddress, from,
                            to, value.ToString(NumberFormatInfo.InvariantInfo));

                        blockIsRelevant = true;
                    }
                    else if (topic == _crcTrustEventTopic && log.LoggersAddress == _circlesHubAddress)
                    {
                        string userAddress = log.Topics[1].ToString().Replace(_addressEmptyBytesPrefix, "0x");
                        string canSendToAddress = log.Topics[2].ToString().Replace(_addressEmptyBytesPrefix, "0x");
                        int limit = new UInt256(log.Data, true).ToInt32(CultureInfo.InvariantCulture);

                        _persistence.AddCirclesTrust(block.Number, txReceipt.TxHash!.ToString(), userAddress,
                            canSendToAddress, limit);

                        blockIsRelevant = true;
                    }
                    else if (topic == _crcHubTransferEventTopic && log.LoggersAddress == _circlesHubAddress)
                    {
                        string fromAddress = log.Topics[1].ToString().Replace(_addressEmptyBytesPrefix, "0x");
                        string toAddress = log.Topics[2].ToString().Replace(_addressEmptyBytesPrefix, "0x");
                        string amount = new UInt256(log.Data, true).ToString(CultureInfo.InvariantCulture);

                        _persistence.AddCirclesHubTransfer(block.Number, txReceipt.TxHash!.ToString(), fromAddress,
                            toAddress, amount);

                        blockIsRelevant = true;
                    }
                    else if (topic == _crcSignupEventTopic && log.LoggersAddress == _circlesHubAddress)
                    {
                        string userAddress = log.Topics[1].ToString().Replace(_addressEmptyBytesPrefix, "0x");
                        string tokenAddress = new Address(log.Data.Slice(12)).ToString(false);

                        _persistence.AddCirclesSignup(block.Number, txReceipt.TxHash!.ToString(), userAddress,
                            tokenAddress);

                        blockIsRelevant = true;
                    }
                    else if (topic == _crcOrganisationSignupEventTopic && log.LoggersAddress == _circlesHubAddress)
                    {
                        string userAddress = log.Topics[1].ToString().Replace(_addressEmptyBytesPrefix, "0x");

                        _persistence.AddCirclesSignup(block.Number, txReceipt.TxHash!.ToString(), userAddress,
                            null);

                        blockIsRelevant = true;
                    }
                }
            }

            if (!blockIsRelevant)
            {
                _persistence.AddIrrelevantBlock(block.Number);
            }
            else
            {
                _persistence.AddVisitedBlock(block.Number);
            }

            // Always return true to continue visiting other blocks
            return BlockVisitOutcome.None;
        }

        public async Task<LevelVisitOutcome> VisitLevelEnd(ChainLevelInfo chainLevelInfo, long levelNumber,
            CancellationToken cancellationToken)
        {
            // Logic to execute at the end of visiting a chain level
            return LevelVisitOutcome.None;
        }
    }
}

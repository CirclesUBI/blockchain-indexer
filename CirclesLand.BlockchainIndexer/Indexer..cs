using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using CirclesLand.BlockchainIndexer.DetailExtractors;
using CirclesLand.BlockchainIndexer.Persistence;
using CirclesLand.BlockchainIndexer.TransactionDetailModels;
using Dapper;
using Nethereum.BlockchainProcessing.BlockStorage.Entities.Mapping;
using Nethereum.Hex.HexTypes;
using Nethereum.Web3;
using Npgsql;

namespace CirclesLand.BlockchainIndexer
{
    public class Indexer
    {
        const string RpcUrl = "https://rpc.circles.land";
        public const string ConnectionString = @"Server=localhost;Port=5432;Database=circles_land_worker;User ID=postgres;Password=postgres;";
        
        private static readonly Web3 _web3 = new(RpcUrl);

        /// <summary>
        /// Checks for new blocks every second. All new blocks are emitted to the stream.
        /// </summary>
        public readonly Source<HexBigInteger, NotUsed> BlockSource = 
            Source.UnfoldAsync(new HexBigInteger(0), async lastBlock =>
            {
                await using var connection = new NpgsqlConnection(ConnectionString);
                connection.Open();
                
                while (true)
                {
                    // Determine if we need to catch up (database old)
                    var currentBlock = await _web3.Eth.Blocks.GetBlockNumber.SendRequestAsync();
                    var lastIndexedBlock = connection.QuerySingleOrDefault<long?>("select max(number) from block") ?? 0;
                    if (lastBlock.Value == 0)
                    {
                        lastBlock = new HexBigInteger(lastIndexedBlock == 0 ? 12540000 : lastIndexedBlock);
                        // lastBlock = new HexBigInteger(lastIndexedBlock == 0 ? 12529458 : lastIndexedBlock);
                    }
                    
                    if (currentBlock.ToLong() > lastIndexedBlock && currentBlock.Value > lastBlock.Value)
                    {
                        var nextBlockToIndex = lastBlock.Value + 1;
                        Console.WriteLine($"Catching up block: {nextBlockToIndex}");
                        
                        return new Option<(HexBigInteger, HexBigInteger)>((new HexBigInteger(nextBlockToIndex), new HexBigInteger(nextBlockToIndex)));
                    }
                    
                    await Task.Delay(500);

                    // At this point we wait for a new block
                    currentBlock = await _web3.Eth.Blocks.GetBlockNumber.SendRequestAsync();
                    if (currentBlock == lastBlock)
                    {
                        continue;
                    }
                    Console.WriteLine($"Got new block: {currentBlock}");

                    return new Option<(HexBigInteger, HexBigInteger)>((currentBlock, currentBlock));
                }
            });

        public void Start()
        {
            var system = ActorSystem.Create("system");
            var materializer = system.Materializer();

            // Check for new blocks every 500ms and emit the block no. every time it changed
            BlockSource
                    
                // Buffer up to 25 block nos. in case that the downstream processing is not fast enough.
                // When the buffer size is exceeded the whole stream will fail.
                // .Buffer(25, OverflowStrategy.Fail) // TODO: This doesn't suite to "catch up" mode
                
                // Get the full block with all transactions
                .SelectAsync(1, currentBlockNo => 
                    _web3.Eth.Blocks
                    .GetBlockWithTransactionsByNumber
                    .SendRequestAsync(currentBlockNo))
                
                // Bundle the every transaction in a block with the block timestamp and send it downstream
                .SelectMany(block =>
                {
                    Console.WriteLine($"  Found {block.Transactions.Length} transactions in block {block.Number}");

                    return block.Transactions
                        .Select(o => (
                            TotalTransactionsInBlock: block.Transactions.Length,
                            Timestamp: block.Timestamp,
                            Transaction: o));
                })
                
                // Add the receipts for every transaction
                .SelectAsync(4, async timestampAndTransaction => 
                {
                    var receipt = await _web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(
                        timestampAndTransaction.Transaction.TransactionHash);
                    
                    return (
                        TotalTransactionsInBlock: timestampAndTransaction.TotalTransactionsInBlock,
                        Timestamp: timestampAndTransaction.Timestamp,
                        Transaction: timestampAndTransaction.Transaction,
                        Receipt: receipt
                    );
                })
                
                // Classify all transactions
                .Select(transactionAndReceipt =>
                {
                    var classification = TransactionClassifier.Classify(
                        transactionAndReceipt.Transaction,
                        transactionAndReceipt.Receipt,
                        null);
                    
                    return (
                        TotalTransactionsInBlock: transactionAndReceipt.TotalTransactionsInBlock,
                        Timestamp: transactionAndReceipt.Timestamp,
                        Transaction: transactionAndReceipt.Transaction,
                        Receipt: transactionAndReceipt.Receipt,
                        Classification: classification
                    );
                })
                
                // Set a flag which indicates whether to store the transaction or not
                .Select(transactionAndReceipt =>
                {
                    var isUnknown = transactionAndReceipt.Classification == TransactionClass.Unknown;
                    if (isUnknown)
                    {
                        Console.WriteLine($"    Tx {transactionAndReceipt.Transaction.TransactionIndex}: Not classified.");
                    }
                    else if (transactionAndReceipt.Classification == TransactionClass.Call)
                    {
                        Console.WriteLine($"    Tx {transactionAndReceipt.Transaction.TransactionIndex}: is a Call without state changes.");
                    }
                    else
                    {
                        Console.WriteLine($"    Tx {transactionAndReceipt.Transaction.TransactionIndex}: {transactionAndReceipt.Classification}");
                    }
                    
                    return (
                        TotalTransactionsInBlock: transactionAndReceipt.TotalTransactionsInBlock,
                        Timestamp: transactionAndReceipt.Timestamp,
                        Transaction: transactionAndReceipt.Transaction,
                        Receipt: transactionAndReceipt.Receipt,
                        Classification: transactionAndReceipt.Classification,
                        ShouldBeIndexed: !isUnknown && transactionAndReceipt.Classification != TransactionClass.Call
                    );
                })
                
                // Add the details for each transaction
                .Select(classifiedTransactions =>
                {
                    var extractedDetails = TransactionDetailExtractor.Extract(
                        classifiedTransactions.Classification,
                        classifiedTransactions.Transaction,
                        classifiedTransactions.Receipt)
                        .ToArray();
                    
                    return (
                        TotalTransactionsInBlock: classifiedTransactions.TotalTransactionsInBlock,
                        TxHash: classifiedTransactions.Transaction.TransactionHash,
                        Timestamp: classifiedTransactions.Timestamp,
                        Transaction: classifiedTransactions.Transaction,
                        Receipt: classifiedTransactions.Receipt,
                        Classification: classifiedTransactions.Classification,
                        SholdBeIndexed: classifiedTransactions.ShouldBeIndexed,
                        Details: extractedDetails
                    );
                })
                
                .RunForeach(transactionWithExtractedDetails =>
                {
                    Console.WriteLine($"      Writing '{transactionWithExtractedDetails.Transaction.TransactionHash}' to the db");

                    using var connection = new NpgsqlConnection(ConnectionString);
                    connection.Open();

                    // "Read committed"-isolation level should be sufficient because the data will not
                    // be updated again once its written.
                    using var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

                    try
                    {
                        var blockTimestamp = ((HexBigInteger) transactionWithExtractedDetails.Timestamp).ToLong();
                        var blockTimestampDateTime = DateTimeOffset.FromUnixTimeSeconds(
                            blockTimestamp).UtcDateTime;

                        var transactionId = new TransactionWriter(connection, transaction).Write(
                            !transactionWithExtractedDetails.SholdBeIndexed,
                            transactionWithExtractedDetails.TotalTransactionsInBlock,
                            blockTimestampDateTime,
                            transactionWithExtractedDetails.Classification,
                            transactionWithExtractedDetails.Transaction,
                            transactionWithExtractedDetails.Details);

                        if (transactionId != null)
                        {
                            var detailIds = new TransactionDetailWriter(connection, transaction).Write(
                                transactionId.Value,
                                transactionWithExtractedDetails.Details);
                        }

                        transaction.Commit();
                        
                        Console.WriteLine($"      Successfully wrote '{transactionWithExtractedDetails.Transaction.TransactionHash}' to the db");
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        Console.WriteLine($"      Failed to write '{transactionWithExtractedDetails.Transaction.TransactionHash}' to the db");
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.StackTrace);
                    }

                }, materializer);
        }
    }

    public static class Program
    {
        public static void Main()
        {
            var c = new Indexer();
            Task.Run(() =>
            {
                c.Start();
            });
            
            new AutoResetEvent(false).WaitOne();
        }
    }
}
using System;
using System.Collections.Concurrent;

namespace CirclesLand.BlockchainIndexer
{
    public static class Statistics
    {
        public static readonly DateTime StartedAt = DateTime.Now;
        
        /// <summary>
        /// The number of all execution errors during the runtime of the process.
        /// </summary>
        public static long TotalErrorCount = 0;

        /// <summary>
        /// The number of continuous errors without any success in between.
        /// This number is used to determine the error backoff time. 
        /// </summary>
        public static long ImmediateErrorCount = 0;

        /// <summary>
        /// The number of total executed rounds.
        /// </summary>
        public static long TotalStartedRounds = 0;

        /// <summary>
        /// The number of total executed rounds.
        /// </summary>
        public static long TotalCompletedRounds = 0;

        public static long TotalDownloadedBlocks = 0;
        public static long TotalDownloadedTransactions = 0;
        public static long TotalDownloadedReceipts = 0;

        private static long LastTotalDownloadedBlocks = 0;
        private static long LastTotalDownloadedTransactions = 0;
        private static long LastTotalDownloadedReceipts = 0;

        private static DateTime LastStatistics = DateTime.Now;
        
        public static long TotalProcessedBatches = 0;


        private static ConcurrentDictionary<long, DateTime> _blockRuntimes = new();

        public static void TrackBlockEnter(long block)
        {
            _blockRuntimes.TryAdd(block, DateTime.Now);
        }
        public static void TrackBlockWritten(long block)
        {
            if (!_blockRuntimes.TryRemove(block, out var startTime))
            {
                return;
            }
            
            var runtime = DateTime.Now - startTime;
            Console.WriteLine($"Block {block} took {runtime} to process.");
        }

        public static void Print()
        {
            Console.WriteLine($"............... {DateTime.Now} ...............");
            
            var defaultColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Cyan;

            var now = DateTime.Now;
            var absoluteRuntime = now - StartedAt;
            var timeBetweenStatCalls = DateTime.Now - LastStatistics;

            var totalBlockDownloadRate = (TotalDownloadedBlocks / absoluteRuntime.TotalSeconds).ToString("F");
            var totalTransactionDownloadRate = (TotalDownloadedTransactions / absoluteRuntime.TotalSeconds).ToString("F");
            var totalReceiptDownloadRate = (TotalDownloadedReceipts / absoluteRuntime.TotalSeconds).ToString("F");
            
            var lastDownloadedBlocks =  TotalDownloadedBlocks - LastTotalDownloadedBlocks;
            var lastDownloadedReceipts =  TotalDownloadedReceipts - LastTotalDownloadedReceipts;
            var lastDownloadedTransactions =  TotalDownloadedTransactions - LastTotalDownloadedTransactions;

            var lastBlockDownloadRate = (lastDownloadedBlocks / timeBetweenStatCalls.TotalSeconds).ToString("F");
            var lastReceiptDownloadRate = (lastDownloadedReceipts / timeBetweenStatCalls.TotalSeconds).ToString("F");
            var lastTransactionDownloadRate = (lastDownloadedTransactions / timeBetweenStatCalls.TotalSeconds).ToString("F");
            
            Console.WriteLine($"{TotalErrorCount} errors in {TotalStartedRounds} started rounds. Last import batch took {timeBetweenStatCalls}.");
            if (TotalCompletedRounds > 0)
            {
                Console.WriteLine($"{TotalCompletedRounds} completed rounds.");
            }

            Console.WriteLine($"Rates:");
            var totalBlocks = ($"{TotalDownloadedBlocks} ({totalBlockDownloadRate}/s)").PadRight(24);
            var lastBlocks = ($"{lastDownloadedBlocks} ({lastBlockDownloadRate}/s)").PadRight(24);
            Console.WriteLine($"* Blocks       Total: {totalBlocks}; Last: {lastBlocks}");
            
            var totalReceipts = ($"{TotalDownloadedReceipts} ({totalReceiptDownloadRate}/s)").PadRight(24);
            var lastReceipts = ($"{lastDownloadedReceipts} ({lastReceiptDownloadRate}/s)").PadRight(24);
            Console.WriteLine($"* Receipts     Total: {totalReceipts}; Last: {lastReceipts}");
            
            var totalTransactions = ($"{TotalDownloadedTransactions} ({totalTransactionDownloadRate}/s)").PadRight(24);
            var lastTransactions = ($"{lastDownloadedTransactions} ({lastTransactionDownloadRate}/s)").PadRight(24);
            Console.WriteLine($"* Transactions Total: {totalTransactions}; Last: {lastTransactions}");
            
            Console.ForegroundColor = defaultColor;

            LastTotalDownloadedBlocks = TotalDownloadedBlocks;
            LastTotalDownloadedReceipts = TotalDownloadedReceipts;
            LastTotalDownloadedTransactions = TotalDownloadedTransactions;
            LastStatistics = DateTime.Now;
        }
    }
}
using System;

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

        public static long TotalProcessedBatches = 0;

        public static void Print()
        {
            Console.WriteLine($"............... {DateTime.Now} ...............");
            
            var defaultColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Cyan;

            var now = DateTime.Now;
            var runtime = now - StartedAt;

            var blockDownloadRate = TotalDownloadedBlocks / runtime.TotalSeconds;
            var transactionDownloadRate = TotalDownloadedTransactions / runtime.TotalSeconds;
            var receiptDownloadRate = TotalDownloadedReceipts / runtime.TotalSeconds;

            Console.WriteLine($"TotalErrorCount:              {TotalErrorCount}                     ");
            Console.WriteLine($"ImmediateErrorCount:          {ImmediateErrorCount}                 ");
            Console.WriteLine($"TotalStartedRounds:           {TotalStartedRounds}                  ");
            Console.WriteLine($"TotalCompletedRounds:         {TotalCompletedRounds}                ");
            Console.WriteLine($"TotalDownloadedBlocks:        {TotalDownloadedBlocks} ({blockDownloadRate}/s)");
            Console.WriteLine($"TotalDownloadedTransactions:  {TotalDownloadedTransactions} ({transactionDownloadRate}/s)");
            Console.WriteLine($"TotalDownloadedReceipts:      {TotalDownloadedReceipts} ({receiptDownloadRate}/s)");
            
            Console.ForegroundColor = defaultColor;
        }
    }
}
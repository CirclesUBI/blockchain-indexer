using System;

namespace CirclesLand.BlockchainIndexer.Util
{
    public static class Logger
    {
        public static void Log(string message)
        {
            Console.WriteLine(message);
        }
        
        public static void LogError(string message)
        {
            var origColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(message);
            Console.ForegroundColor = origColor;
        }
    }
}
using System;
using System.Threading;
using CirclesLand.BlockchainIndexer.Persistence;
using CirclesLand.BlockchainIndexer.Sources;
using CirclesLand.BlockchainIndexer.Util;
using Nethereum.Web3;
using Npgsql;

namespace CirclesLand.BlockchainIndexer
{
    public class RoundContext : IDisposable
    {
        public class RoundErrorEventArgs : EventArgs
        {
            public Exception Exception { get; }

            public RoundErrorEventArgs(Exception exception)
            {
                Exception = exception;
            }
        }

        public DateTime CreatedAt { get; }
        public DateTime StartAt { get; }
        public long RoundNo { get; }
        public NpgsqlConnection Connection { get; }
        public Web3 Web3 { get; }
        public SourceFactory SourceFactory { get; }

        public event EventHandler<RoundErrorEventArgs>? Error;
        public event EventHandler? Disposed;
        public event EventHandler? BatchSuccess;

        public RoundContext(long number, NpgsqlConnection connection, Web3 web3, TimeSpan penalty)
        {
            RoundNo = number;
            CreatedAt = DateTime.Now;
            StartAt = CreatedAt + penalty;
            Connection = connection;
            Web3 = web3;
            SourceFactory = new SourceFactory();
        }

        public void Log(string message)
        {
            Logger.Log($"Round {RoundNo}: {message}");
        }

        public long GetLastValidBlock()
        {
            return BlockTracker.GetLastValidBlock(Connection);
        }

        public void OnError(Exception exception)
        {
            Logger.LogError($"Round {RoundNo}: {exception.Message}");
            Logger.LogError($"Round {RoundNo}: {exception.StackTrace}");

            Error?.Invoke(this, new RoundErrorEventArgs(exception));
        }

        public void OnBatchSuccess()
        {
            BatchSuccess?.Invoke(this, EventArgs.Empty);
            Interlocked.Increment(ref Statistics.TotalProcessedBatches);
            Statistics.Print();
        }

        public void Dispose()
        {
            Connection.Dispose();
            Disposed?.Invoke(this, EventArgs.Empty);
        }
    }
}
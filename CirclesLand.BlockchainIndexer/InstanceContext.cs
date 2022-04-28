using System;
using System.Collections.Concurrent;
using System.Threading;
using Npgsql;

namespace CirclesLand.BlockchainIndexer
{
    public class InstanceContext
    {
        private readonly ConcurrentDictionary<long, RoundContext> _rounds = new();

        private NpgsqlConnection GetConnection()
        {
            return new(Settings.ConnectionString);
        }

        public RoundContext CreateRoundContext()
        {
            var connection = GetConnection();
            connection.Open();
            
            var roundNo = Interlocked.Increment(ref Statistics.TotalStartedRounds);

            var penalty = Settings.ErrorRestartPenaltyInMs *
                          (Statistics.ImmediateErrorCount * Statistics.ImmediateErrorCount);
            penalty = penalty > Settings.MaxErrorRestartPenaltyInMs
                ? Settings.MaxErrorRestartPenaltyInMs
                : penalty;

            var round = new RoundContext(roundNo, connection, TimeSpan.FromMilliseconds(penalty));

            round.BatchSuccess += RoundOnBatchSuccess;
            round.Error += RoundOnError;
            round.Disposed += RoundOnDisposed;

            _rounds.TryAdd(roundNo, round);

            return round;
        }

        private void RoundOnBatchSuccess(object? sender, EventArgs e)
        {
            Statistics.ImmediateErrorCount = 0;
        }

        private void RoundOnError(object? sender, RoundContext.RoundErrorEventArgs e)
        {
            Interlocked.Increment(ref Statistics.ImmediateErrorCount);
            Interlocked.Increment(ref Statistics.TotalErrorCount);
        }

        private void RoundOnDisposed(object? sender, EventArgs e)
        {
            if (sender is not RoundContext round)
            {
                return;
            }

            if (!_rounds.TryRemove(round.RoundNo, out var removedRound))
            {
                return;
            }

            removedRound.BatchSuccess -= RoundOnBatchSuccess;
            removedRound.Error -= RoundOnError;
            removedRound.Disposed -= RoundOnDisposed;

            Interlocked.Increment(ref Statistics.TotalCompletedRounds);
        }
    }
}
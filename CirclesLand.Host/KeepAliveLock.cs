using System;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace CirclesLand.Host
{
    [Serializable]
    public class CouldNotAcquireLockException : Exception
    {
        public CouldNotAcquireLockException()
        {
        }

        public CouldNotAcquireLockException(string message) : base(message)
        {
        }

        public CouldNotAcquireLockException(string message, Exception inner) : base(message, inner)
        {
        }

        protected CouldNotAcquireLockException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
    
    public class KeepAliveLock
    {
        public const int LockLifetimeInSeconds = 1;
        
        private readonly TimeSpan _lockDuration = TimeSpan.FromSeconds(LockLifetimeInSeconds);
        private readonly TimeSpan _keepAliveInterval = TimeSpan.FromSeconds(LockLifetimeInSeconds / 2.0);

        private readonly string _lockName;

        private readonly Participant _participant;

        private Timer? _keepAliveTimer;
        private TaskCompletionSource? _keepAliveTaks;
        private string? _lockOwner;
        private CancellationToken? _cancellationToken;
        private ILogger _logger;

        private bool _isRunning = false;

        public KeepAliveLock(Participant participant, string name, ILogger logger)
        {
            _participant = participant;
            _lockName = name;
            _logger = logger;
        }

        /// <summary>
        /// Executes a task while keeping a lock.
        /// </summary>
        /// <param name="task"></param>
        /// <param name="acquireTimeout"></param>
        /// <param name="cancellationToken"></param>
        /// <exception cref="InvalidOperationException"></exception>
        /// <exception cref="Exception"></exception>
        public async Task RunWithLock(
            Func<Task> task,
            TimeSpan? acquireTimeout = null,
            CancellationToken? cancellationToken = null)
        {
            if (_isRunning)
            {
                throw new InvalidOperationException("The lock is already running a task.");
            }

            _isRunning = true;
            try
            {
                _logger.LogDebug(
                    $"Starting a task which requires the '{_lockName}' lock .. Timeout in: {acquireTimeout}.");

                _lockOwner = await _participant.TryAcquireLock(
                    _lockName,
                    _lockDuration,
                    acquireTimeout,
                    cancellationToken);
                
                _isRunning = cancellationToken?.IsCancellationRequested ?? true;

                if (_lockOwner != _participant.InstanceId)
                {
                    throw new CouldNotAcquireLockException(
                        $"Couldn't acquire lock '{_lockName}' after waiting for {acquireTimeout}.");
                }

                _logger.LogDebug($"Acquired lock '{_lockName}', setting the keepAlive timer and starting the task ..");

                _keepAliveTimer = new Timer(OnKeepAlive, null, _keepAliveInterval, _keepAliveInterval);
                _keepAliveTaks = new TaskCompletionSource();

                var cancelPayloadExecutionSource = new CancellationTokenSource();
                var payloadExecutorTask = Task.Run(async () =>
                    {
                        try
                        {
                            await task();
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, $"The payload task execution in lock '{_lockName}' failed.");
                            throw;
                        }
                    }
                    , cancelPayloadExecutionSource.Token);

                var payloadExecutionCancellingKeepAliveTask = _keepAliveTaks.Task.ContinueWith((_) =>
                {
                    cancelPayloadExecutionSource.Cancel();
                });

                var result = await Task.WhenAny(new[]
                {
                    payloadExecutionCancellingKeepAliveTask,
                    payloadExecutorTask,
                });

                if (result.Exception != null)
                {
                    throw result.Exception;
                }
            }
            catch (CouldNotAcquireLockException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"RunWithLock (name: {_lockName}) experienced an error:");
                throw;
            }
            finally
            {
                _isRunning = false;

                if (_keepAliveTimer != null)
                    await _keepAliveTimer.DisposeAsync();

                Release();
            }
        }

        private void Release()
        {
            _participant.Release(_lockName);
        }

        private async void OnKeepAlive(object _)
        {
            try
            {
                if (!_isRunning)
                {
                    return;
                }
                
                _lockOwner = await _participant.TryAcquireLock(
                    _lockName,
                    _lockDuration,
                    TimeSpan.Zero);

                if (_lockOwner != _participant.InstanceId)
                {
                    throw new Exception($"Couldn't keep the lock '{_lockName}' alive and lost it.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"An error occurred in the keep alive timer for lock '{_lockName}':");
                _keepAliveTaks?.TrySetException(ex);
            }
        }
    }
}
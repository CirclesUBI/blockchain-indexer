using System;
using System.Collections.Concurrent;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Dapper;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;
using Timer = System.Timers.Timer;

namespace CirclesLand.Host
{
    public class Participant
    {
        public string InstanceId { get; private set; }

        private readonly string _connectionString;
        private readonly string _prefix;
        private readonly TimeSpan _keepAliveInterval;

        private readonly DatabaseEventListener _databaseServiceEventListener;
        private readonly DatabaseEventListener _databaseSignalEventListener;

        public ConcurrentDictionary<string, DateTime> KnownInstances { get; } = new();

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, string?>> _knownSignalsByInstance =
            new();

        private bool _isRunning = false;
        private bool _isPrimary = false;
        private DateTime? _lastTimeout = null;
        private Timer _timer;
        private Exception? _lastError;
        private ILogger _logger;
        private string? _hostId;

        public event UnhandledExceptionEventHandler Error;

        private const int AcquireSpinWaitTime = (KeepAliveLock.LockLifetimeInSeconds * 1000) / 2;
        private readonly TimeSpan _primaryLockTimeout = TimeSpan.FromSeconds(KeepAliveLock.LockLifetimeInSeconds);

        private ConcurrentDictionary<
            (string? guid, string? serviceId, string signalName)
            , TaskCompletionSource<string?>> _waitingForSignal = new();

        public static Participant Create(string connectionString, TimeSpan keepAliveInterval, string prefix)
        {
            var databaseServiceEventListener = DatabaseEventListener.Create(connectionString, "service");
            var databaseSignalEventListener = DatabaseEventListener.Create(connectionString, "signal");

            var participant = new Participant(
                connectionString,
                keepAliveInterval,
                prefix,
                databaseServiceEventListener,
                databaseSignalEventListener);

            return participant;
        }

        private Participant(
            string connectionString,
            TimeSpan keepAliveInterval,
            string prefix,
            DatabaseEventListener databaseServiceEventListener,
            DatabaseEventListener databaseSignalEventListener)
        {
            _connectionString = connectionString;
            _keepAliveInterval = keepAliveInterval;
            _prefix = prefix;

            _databaseServiceEventListener = databaseServiceEventListener;
            _databaseSignalEventListener = databaseSignalEventListener;

            _timer = new Timer(1000);
            InstanceId = (_prefix ?? "") + "_" + Guid.NewGuid().ToString("N");
        }

        public async Task Start(ILogger logger, string hostId)
        {
            _logger = logger;
            _hostId = hostId;

            ThrowIfError();

            if (_isRunning)
                throw new InvalidOperationException("The Coordinator is already running.");

            await Register(_hostId);

            _timer.Start();
            _timer.Elapsed += OnTimerElapsed;

            _logger.LogDebug("starting _databaseServiceEventListener");
            _databaseServiceEventListener.Start();
            _databaseServiceEventListener.Data += OnServiceEvent;
            _databaseServiceEventListener.Error += OnDbEventListenerError;

            _logger.LogDebug("starting _databaseSignalEventListener");
            _databaseSignalEventListener.Start();
            _databaseSignalEventListener.Data += OnSignalEvent;
            _databaseServiceEventListener.Error += OnDbEventListenerError;

            _logger.LogDebug("initially getting a list of all running services ..");
            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            var serviceList = await connection.QueryAsync(
                @"select service_id, timeout_at, resource_type, resource_id, resource_name, resource_value, is_primary
                   from active_services");

            foreach (var o in serviceList)
            {
                if (o.resource_type == "signal")
                {
                    try
                    {
                        AddOrUpdateSignal(o.service_id, o.resource_name, o.resource_value, o.is_primary ?? false);
                    }
                    catch (Exception exception)
                    {
                        _logger.LogError(exception, $"Couldn't AddOrUpdateSignal.");
                        _logger.LogError("{0}", JsonConvert.SerializeObject((object) o));
                    }
                }

                KnownInstances.TryAdd(o.service_id, o.timeout_at);
            }

            PrintServiceList();

            _isRunning = true;
            _logger.LogDebug("started participant");
        }

        private void AddOrUpdateSignal(string serviceId, string signalName, string? signalValue, bool isPrimary)
        {
            if (isPrimary)
            {
                foreach (var instance in _knownSignalsByInstance)
                {
                    if (instance.Value.ContainsKey(signalName))
                    {
                        instance.Value.TryRemove(signalName, out _);
                    }
                }
            }

            var serviceHasSignals = _knownSignalsByInstance.TryGetValue(
                serviceId,
                out ConcurrentDictionary<string, string?> currentSignalsOfService);

            if (!serviceHasSignals)
            {
                currentSignalsOfService = new();
                _knownSignalsByInstance.TryAdd(serviceId, currentSignalsOfService);
            }

            currentSignalsOfService.AddOrUpdate(signalName, signalValue, (_, _) => signalValue);

            foreach (var waitingTask in _waitingForSignal.ToArray())
            {
                if (waitingTask.Key.serviceId == null 
                    && waitingTask.Key.signalName == signalName
                    && _waitingForSignal.TryRemove(waitingTask.Key, out var taskToComplete1))
                {
                    taskToComplete1.SetResult(signalValue);
                }
                
                if (waitingTask.Key.serviceId == null)
                {
                    continue;
                }
                
                if (!_knownSignalsByInstance.TryGetValue(waitingTask.Key.serviceId, out var signalsOfService))
                {
                    continue;
                }

                if (!signalsOfService.TryGetValue(waitingTask.Key.signalName, out var requestedSignalValue))
                {
                    continue;
                }

                if (_waitingForSignal.TryRemove(waitingTask.Key, out var taskToComplete2))
                {
                    taskToComplete2.SetResult(requestedSignalValue);
                }
            }
        }

        private void OnDbEventListenerError(object sender, UnhandledExceptionEventArgs e)
        {
            _lastError = (Exception) e.ExceptionObject;
            Error?.Invoke(this, new UnhandledExceptionEventArgs(_lastError, false));
        }

        private async Task Register(string hostId)
        {
            ThrowIfError();

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            _lastTimeout = DateTime.UtcNow + _keepAliveInterval;
            await connection.ExecuteAsync("call register_service(@service_id, @host_id, @timeout_at);", new
            {
                service_id = InstanceId,
                host_id = hostId,
                timeout_at = _lastTimeout
            });
        }

        private void OnSignalEvent(object sender, NpgsqlNotificationEventArgs e)
        {
            dynamic o = JObject.Parse(e.Payload);
            long id = o.id;
            
            DateTime timestamp = o.timestamp;
            string serviceId = o.service_id;
            string name = o.name;
            string? value = o.value;
            bool? isPrimary = o.is_primary;

            if (string.IsNullOrEmpty(name))
            {
                _logger.LogWarning($"Received a signal without name: {e.Payload}");
            }
            else
            {
                AddOrUpdateSignal(serviceId, name, value, isPrimary ?? false);
            }

            if (serviceId == InstanceId)
            {
                return;
            }

            // PrintServiceList();
        }

        private void ThrowIfError()
        {
            if (_lastError == null)
                return;

            throw new Exception(
                "A component of the Participant encountered an error. See inner exception for details.",
                _lastError);
        }
        
        private void OnServiceEvent(object sender, NpgsqlNotificationEventArgs e)
        {
            dynamic o = JObject.Parse(e.Payload);
            long id = o.id;
            
            var timestamp = (DateTime) o.timestamp;
            var type = (string) o.type;
            var serviceId = (string) o.service_id;
            var timeoutAt = type != "disconnect" ? (DateTime?) o.timeout_at : null;
            var now = DateTime.UtcNow;

            var previousServices = KnownInstances.Keys.ToHashSet();
            if (type == "connected" && timeoutAt != null)
            {
                if (KnownInstances.TryRemove(serviceId, out var oldTimeout))
                {
                    if (oldTimeout != DateTime.MinValue && oldTimeout < now)
                    {
                        Console.WriteLine(
                            $"Warning: Service '{serviceId}' timed out at {oldTimeout} before it just re-appeared at {now} with new timeout at {timeoutAt}.");
                    }
                }
                KnownInstances.TryAdd(serviceId, timeoutAt.Value);
            }
            else if (type == "disconnected")
            {
                KnownInstances.TryRemove(serviceId, out var oldTimeout);
            }

            var currentServices = KnownInstances.Keys.ToHashSet();
            if (previousServices.Count != currentServices.Count)
            {
                PrintServiceList();
            }
        }

        private void PrintServiceList()
        {
            var sb = new StringWriter();
            sb.WriteLine($"New service list:");
            foreach (var currentService in KnownInstances.Keys.OrderBy(o => o))
            {
                var isSelf = currentService == InstanceId;
                var isSameHost = false;

                sb.WriteLine($"  {currentService}{(isSelf ? " (self)" : "")}");
                if (_knownSignalsByInstance.ContainsKey(currentService))
                {
                    foreach (var keyValuePair in _knownSignalsByInstance[currentService].OrderBy(o => o.Key))
                    {
                        sb.WriteLine($"    Signal: {keyValuePair.Key}, Value: {keyValuePair.Value}");
                    }
                }
            }

            _logger.LogInformation(sb.ToString());
        }

        private async void OnTimerElapsed(object sender, ElapsedEventArgs e)
        {
            try
            {
                // Clear all timed-out services
                var now = DateTime.UtcNow;
                var previousCount = KnownInstances.Count;

                KnownInstances
                    .ToArray()
                    .Where(o => o.Value <= now)
                    .Select(o => o.Key)
                    .ToList()
                    .ForEach(timedOutInstance => KnownInstances.TryRemove(timedOutInstance, out _));

                if (previousCount != KnownInstances.Count)
                {
                    PrintServiceList();
                }

                if (_lastTimeout - TimeSpan.FromSeconds(_keepAliveInterval.TotalSeconds / 2.0) > now)
                {
                    return;
                }

                // Need to refresh the keep alive signal (half time to timeout)
                await Register(_hostId);
            }
            catch (Exception ex)
            {
                Util.PrintException(ex);
                _lastError = ex;
                Error?.Invoke(this, new UnhandledExceptionEventArgs(_lastError, false));
            }
        }

        public async Task Stop()
        {
            ThrowIfError();

            if (!_isRunning)
                throw new InvalidOperationException("The Coordinator is not running.");

            _timer.Elapsed -= OnTimerElapsed;
            _timer.Stop();

            _databaseServiceEventListener.Data -= OnServiceEvent;
            _databaseServiceEventListener.Error -= OnDbEventListenerError;
            _databaseServiceEventListener.Stop(1000);

            _databaseSignalEventListener.Data -= OnSignalEvent;
            _databaseSignalEventListener.Error -= OnDbEventListenerError;
            _databaseSignalEventListener.Stop(1000);

            _isRunning = false;
        }

        public async Task DeleteService()
        {
            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            await using var transaction = await connection.BeginTransactionAsync();

            await connection.ExecuteAsync("call delete_service(@service_id);"
                , new {service_id = InstanceId}
                , transaction);

            await transaction.CommitAsync();
        }

        string? _lastPrimaryLockOwner = null;

        /// <summary>
        /// Runs until a node emitted the only 'primary' signal (or until this node becomes a primary itself).
        /// </summary>
        public async Task<string> RaceForPrimaryNode(CancellationToken cancellationToken, string primaryLockName)
        {
            string? publishedPrimaryLockOwner = null;
            var sqlExceptions = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogDebug("{0} is trying to acquire the '{1}' lock ..", InstanceId, primaryLockName);

                string? primaryLockOwner = null;
                primaryLockOwner = await TryAcquireLock(
                    primaryLockName,
                    _primaryLockTimeout,
                    TimeSpan.Zero,
                    null,
                    primaryLockName);

                if (primaryLockOwner == null)
                {
                    sqlExceptions++;
                    if (sqlExceptions >= 3)
                    {
                        throw new Exception(
                            $"Couldn't acquire a log for '{primaryLockName}' but also couldn't find out who holds the lock.");
                    }

                    continue;
                }

                var isPrimary = primaryLockOwner == InstanceId;

                if (publishedPrimaryLockOwner != null && publishedPrimaryLockOwner != primaryLockOwner)
                {
                    publishedPrimaryLockOwner = null;
                }

                if (publishedPrimaryLockOwner != null)
                {
                    if (_lastPrimaryLockOwner != publishedPrimaryLockOwner)
                    {
                        _logger.LogInformation($"Agreed on {primaryLockOwner} as '{primaryLockName}' node.",
                            publishedPrimaryLockOwner);
                    }

                    _lastPrimaryLockOwner = publishedPrimaryLockOwner;
                    return publishedPrimaryLockOwner;
                }

                if (isPrimary)
                {
                    _logger.LogInformation($"{InstanceId} is now the {primaryLockName} process.");

                    return InstanceId;
                }

                cancellationToken.ThrowIfCancellationRequested();

                var gotSignal = await WaitForServiceSignal(primaryLockOwner, primaryLockName,
                    TimeSpan.FromSeconds(1),
                    cancellationToken);

                if (gotSignal)
                {
                    _logger.LogDebug("{0} emitted the '{1}' signal.",
                        primaryLockOwner, primaryLockName);

                    publishedPrimaryLockOwner = primaryLockOwner;
                }
                else
                {
                    publishedPrimaryLockOwner = null;
                }
            }

            throw new OperationCanceledException("RaceForPrimaryNode() was cancelled");
        }

        public async Task<string?> TryGetLockOwner(string name)
        {
            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            return await connection.QuerySingleOrDefaultAsync<string?>("select try_get_lock_owner(@name)", new
            {
                name = name
            });
        }

        /// <summary>
        /// Creates a global lock or waits until an existing lock is released or timed-out.
        /// Returns 'true' if the lock was acquired.
        /// </summary>
        public async Task<string?> TryAcquireLock(
            string name,
            TimeSpan? lockTimeout,
            TimeSpan? acquireTimeout = null,
            CancellationToken? cancellationToken = null,
            string? signal = null,
            string? signalValue = null)
        {
            ThrowIfError();

            cancellationToken?.ThrowIfCancellationRequested();

            var acquireTimeoutAt = DateTime.UtcNow + acquireTimeout;
            var waitIterations = 0;

            string? lastLockOwner = null;
            var sqlExceptions = 0;

            do
            {
                try
                {
                    await using var connection = new NpgsqlConnection(_connectionString);
                    await (cancellationToken != null
                        ? connection.OpenAsync(cancellationToken.Value)
                        : connection.OpenAsync());

                    await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead);

                    lastLockOwner = connection.QuerySingle<string>(
                        "select try_acquire_lock(@service_id, @name, @timeout_at)", new
                        {
                            service_id = InstanceId,
                            name = name,
                            timeout_at = lockTimeout == null
                                ? null
                                : DateTime.UtcNow + lockTimeout
                        }, transaction);

                    if (lastLockOwner == InstanceId && signal != null)
                    {
                        await SetSignal(connection, transaction, true, signal, signalValue);
                    }

                    await transaction.CommitAsync();
                }
                catch (NpgsqlException ex)
                {
                    sqlExceptions++;
                    _logger.LogWarning("Couldn't acquire a log because of an sql exception. Tries left: {0}",
                        3 - sqlExceptions);

                    if (DateTime.UtcNow > acquireTimeoutAt)
                    {
                        return null;
                    }

                    if (sqlExceptions >= 3)
                    {
                        throw;
                    }

                    // Wait a randomized amount of time before retry
                    await Task.Delay(new Random().Next(0, 250));
                    continue;
                }

                if (lastLockOwner == InstanceId)
                {
                    return lastLockOwner;
                }

                if (++waitIterations % 100 == 0)
                {
                    Console.WriteLine($"Warning: 100 tries to acquire the lock for '{name}' failed. " +
                                      $"Repeating until: {acquireTimeoutAt?.ToString() ?? "<infinity>"}");
                }

                if (cancellationToken?.IsCancellationRequested != true)
                {
                    await Task.Delay(AcquireSpinWaitTime);
                }
            }
            // Loop when the acquire timeout lies in the future or none is set
            while (cancellationToken?.IsCancellationRequested != true
                   && (acquireTimeoutAt == null || acquireTimeoutAt.Value > DateTime.UtcNow));

            return lastLockOwner;
        }

        public async Task<bool> WaitForServiceSignal(
            string serviceId,
            string name,
            TimeSpan? waitTimeout = null,
            CancellationToken? cancellationToken = null)
        {
            ThrowIfError();

            var waitTimeoutAt = DateTime.UtcNow + waitTimeout;
            do
            {
                await using var connection = new NpgsqlConnection(_connectionString);
                await (cancellationToken != null
                    ? connection.OpenAsync(cancellationToken.Value)
                    : connection.OpenAsync());

                var isServiceSignalSet = await connection.QuerySingleAsync<bool>(
                    "select is_service_signal_set(@service_id, @name)", new
                    {
                        service_id = serviceId,
                        name = name
                    });

                if (isServiceSignalSet)
                {
                    return true;
                }

                if (waitTimeout != null && waitTimeoutAt.Value > DateTime.UtcNow)
                {
                    return false;
                }

                await Task.Delay(AcquireSpinWaitTime);
            } while (cancellationToken?.IsCancellationRequested != true);

            return false;
        }

        public async Task<string?> WaitForServiceSignalValue(
            string? serviceId,
            string name,
            TimeSpan? waitTimeout = null,
            CancellationToken? cancellationToken = null)
        {
            ThrowIfError();

            var waitTimeoutAt = DateTime.UtcNow + waitTimeout;

            do
            {
                var taskCompletionSource = new TaskCompletionSource<string>();
                var waitForEventTask = taskCompletionSource.Task;
                var guid = Guid.NewGuid().ToString("N");
                var signalKey = (guid, serviceId, name);
                var now = DateTime.UtcNow;

                if (!_waitingForSignal.TryAdd(signalKey, taskCompletionSource))
                {
                    continue;
                }

                if (waitTimeout != null && waitTimeoutAt.Value <= now)
                {
                    _waitingForSignal.TryRemove(signalKey, out _);
                    return null;
                }

                var remainingTimeUntilTimeout = waitTimeoutAt - now
                                                ?? TimeSpan.FromSeconds(AcquireSpinWaitTime);

                await Task.WhenAny(waitForEventTask, Task.Delay(remainingTimeUntilTimeout));

                if (!waitForEventTask.IsCompleted)
                {
                    _waitingForSignal.TryRemove(signalKey, out _);
                }
                else
                {
                    return waitForEventTask.Result;
                }
            } while (cancellationToken?.IsCancellationRequested != true);

            return null;
        }

        public async Task SetSignal(bool isPrimary, string name, string? value)
        {
            ThrowIfError();
            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead);
            await SetSignal(connection, transaction, isPrimary, name, value);
            await transaction.CommitAsync();
        }

        public async Task SetSignal(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            bool isPrimary,
            string name,
            string? value)
        {
            ThrowIfError();

            await connection.ExecuteAsync(
                "call set_signal(@service_id, @is_primary, @name, @value)", new
                {
                    service_id = InstanceId,
                    is_primary = isPrimary,
                    name = name,
                    value = value
                }, transaction);
        }

        public async Task DeleteSignal(NpgsqlConnection connection, NpgsqlTransaction transaction, string name)
        {
            ThrowIfError();

            await connection.ExecuteAsync(
                "call delete_signal(@service_id, @name)", new
                {
                    service_id = InstanceId,
                    name = name
                }, transaction);
        }

        /// <summary>
        /// Releases a lock hold by this instance.
        /// </summary>
        public async void Release(string name)
        {
            ThrowIfError();
            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            await connection.ExecuteAsync("delete from lock where name = @name and service_id = @service_id", new
            {
                name = name,
                service_id = InstanceId
            }, transaction);
            await transaction.CommitAsync();
        }

        /// <summary>
        /// Releases all locks hold by this instance.
        /// </summary>
        public void ReleaseAll()
        {
            ThrowIfError();
        }
    }
}
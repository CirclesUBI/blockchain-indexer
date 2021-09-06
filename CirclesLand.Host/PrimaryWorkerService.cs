using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CirclesLand.Host
{
    public abstract class PrimaryWorkerService : ParticipantHostedService
    {
        protected bool IsPrimary => _primaryServiceId == Participant.InstanceId;

        private string _primaryServiceId;
        private Timer? _refreshPrimaryLockTimer;
        private CancellationTokenSource _serverLoopCancellationSource = new();

        public abstract string PrimaryLockName { get; }
        public abstract bool CanEmit { get; }
        public abstract bool CanProcess { get; }

        public PrimaryWorkerService(
            string prefix,
            IHostApplicationLifetime applicationLifetime,
            ILogger<ParticipantHostedService> logger)
            : base(prefix, applicationLifetime, logger)
        {
        }

        protected abstract Task<string> Emit(CancellationToken cancellationToken);

        protected virtual async Task EmitInternal(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var value = await Emit(cancellationToken);
                if (value == null)
                {
                    continue;
                }

                await Participant.SetSignal(true, "work", value);

                Logger.LogInformation($"{Participant.InstanceId} emitted {value}");
            }
        }

        protected abstract Task Process(string payload, CancellationToken cancellationToken);

        protected virtual async Task ProcessInternal(CancellationToken cancellationToken)
        {
            DateTime? lastTaskAt = null;
            var idleYieldInterval = TimeSpan.FromSeconds(1);

            while (!cancellationToken.IsCancellationRequested)
            {
                if (_primaryServiceId != null && !Participant.KnownInstances.ContainsKey(_primaryServiceId))
                {
                    Logger.LogWarning("Primary service '{0}' died unexpectedly.",
                        _primaryServiceId);

                    _primaryServiceId = null;
                    await Task.Delay(1000, cancellationToken);
                    break;
                }


                if (lastTaskAt != null && DateTime.UtcNow - lastTaskAt > idleYieldInterval)
                {
                    break;
                }

                if (_primaryServiceId != null)
                {
                    Logger.LogInformation("{0} is waiting for 'work' signals from '{1} ..",
                        Participant.InstanceId,
                        _primaryServiceId);

                    var signalValueOrNull = await Participant.WaitForServiceSignalValue(
                        _primaryServiceId,
                        "work",
                        idleYieldInterval,
                        cancellationToken);

                    if (signalValueOrNull != null)
                    {
                        Logger.LogInformation($"Processing task {signalValueOrNull} ...");

                        try
                        {
                            new KeepAliveLock(Participant, signalValueOrNull, Logger)
                                .RunWithLock(async () => await Process(signalValueOrNull, cancellationToken),
                                    TimeSpan.Zero,
                                    cancellationToken)
                                .Wait(cancellationToken);
                        }
                        catch (AggregateException ex)
                            when (ex.InnerExceptions.SingleOrDefault(o => o is TaskCanceledException) != null)
                        {
                            Logger.LogWarning("The processing of task '{0}' was cancelled.");
                        }
                        catch (AggregateException ex)
                            when (ex.InnerExceptions.SingleOrDefault(o => o is CouldNotAcquireLockException) != null)
                        {
                            Logger.LogInformation("Another process took task {0}.", signalValueOrNull);
                        }

                        Logger.LogDebug("Processing round finished.");
                        lastTaskAt = DateTime.UtcNow;
                    }
                    else
                    {
                        Logger.LogDebug("Processing round finished without a task.");
                    }
                }
            }
        }

        public override async Task OnStart(CancellationToken cancellationToken)
        {
            if (!CanEmit && !CanProcess)
            {
                throw new Exception($"{GetType().Name} cannot emit and cannot process. The implementation is invalid.");
            }

            var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
                _serverLoopCancellationSource.Token,
                cancellationToken);

#pragma warning disable 4014
            Task.Run(async () =>
                {
                    while (!linkedCancellationTokenSource.Token.IsCancellationRequested)
                    {
                        if (CanEmit)
                        {
                            _primaryServiceId =
                                await Participant.RaceForPrimaryNode(cancellationToken, PrimaryLockName);
                        }
                        else
                        {
                            _primaryServiceId = await Participant.TryGetLockOwner(PrimaryLockName);
                        }

                        if (_primaryServiceId == null)
                        {
                            Logger.LogWarning(
                                "{0} is waiting for a '{1}' instance. Retrying in 500 ms",
                                Participant.InstanceId,
                                PrimaryLockName);

                            await Task.Delay(500, linkedCancellationTokenSource.Token);

                            continue;
                        }


                        if (CanProcess && CanEmit)
                        {
                            Logger.LogInformation("{0} is running as '{1}'."
                                , Participant.InstanceId
                                , IsPrimary ? PrimaryLockName : "worker");
                        }
                        else if (CanProcess)
                        {
                            Logger.LogInformation("{0} is running as '{1}'."
                                , Participant.InstanceId
                                , "worker");
                        }
                        else if (CanEmit)
                        {
                            Logger.LogInformation("{0} is running as '{1}'."
                                , Participant.InstanceId
                                , IsPrimary ? PrimaryLockName : "primary (standby)");
                        }

                        if (IsPrimary && CanEmit)
                        {
                            try
                            {
                                linkedCancellationTokenSource.Token.ThrowIfCancellationRequested();
                                await new KeepAliveLock(Participant, PrimaryLockName, Logger)
                                    .RunWithLock(
                                        () => EmitInternal(linkedCancellationTokenSource.Token)
                                        , TimeSpan.Zero);
                            }
                            catch (CouldNotAcquireLockException)
                            {
                                Logger.LogWarning($"{Participant.InstanceId} lost the 'primary' lock.");
                            }
                        }
                        else if (!IsPrimary && CanProcess)
                        {
                            linkedCancellationTokenSource.Token.ThrowIfCancellationRequested();
                            await ProcessInternal(linkedCancellationTokenSource.Token);

                            if (_primaryServiceId == null)
                            {
                                
                            }
                        }
                    }
                }, linkedCancellationTokenSource.Token)
                .ContinueWith(t =>
#pragma warning restore 4014
                {
                    if (t.Exception == null) return;

                    Logger.LogError(t.Exception, "A fatal error occurred:");
                    ApplicationLifetime.StopApplication();
                }, cancellationToken);
        }

        public async override Task OnStop(CancellationToken cancellationToken)
        {
            _serverLoopCancellationSource.Cancel();
            Logger.LogDebug("Goodbye from IndexerHostService {0}", Participant.InstanceId);
        }
    }
}
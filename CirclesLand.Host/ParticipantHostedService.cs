using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CirclesLand.Host
{
    public abstract class ParticipantHostedService 
    {
        public IHostApplicationLifetime ApplicationLifetime { get; }
        public  ILogger Logger { get; }
        
        protected readonly string _prefix;

        protected Participant Participant { get; private set; }
        private const int ParticipantTimeoutInSeconds = 10;

        public ParticipantHostedService(
            string prefix,
            IHostApplicationLifetime applicationLifetime,
            ILogger logger)
        {
            _prefix = prefix;
            ApplicationLifetime = applicationLifetime;
            Logger = logger;
        }

        public async Task StartAsync(CancellationToken _)
        {
            Participant = Participant.Create(
                $"Server=localhost;Port=5432;Database=coordinator;User ID=postgres;Password=postgres;Command Timeout={ParticipantTimeoutInSeconds};",
                TimeSpan.FromSeconds(ParticipantTimeoutInSeconds),
                _prefix);

            Participant.Error += async (s, e) =>
            {
                await StopAsync(_);
                ApplicationLifetime.StopApplication();
                var ex = new Exception("A component of the Participant crashed. See inner exception for details.",
                    (Exception) e.ExceptionObject);
                Logger.LogError(ex, "");
                throw ex;
            };

            Logger.LogDebug("Starting service {0} ..", Participant.InstanceId);

            await Participant.Start(Logger, Environment.MachineName + "/" + Process.GetCurrentProcess().Id.ToString());
            await OnStart(_);

            Logger.LogInformation("Service {0} is running.", Participant.InstanceId);
        }

        public abstract Task OnStart(CancellationToken cancellationToken);
        public abstract Task OnStop(CancellationToken cancellationToken);

        public async Task StopAsync(CancellationToken _)
        {
            Logger.LogDebug("Stopping service {0} ..", Participant.InstanceId);

            await Participant.Stop();
            await OnStop(_);

            await Participant.DeleteService();

            Logger.LogInformation("Service {0} is stopped.", Participant.InstanceId);
        }
    }
}
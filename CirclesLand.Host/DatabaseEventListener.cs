using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace CirclesLand.Host
{
    public class DatabaseEventListener
    {
        private readonly string _connectionString;
        private readonly string _topic;
        
        private Thread _thread;
        private bool _stopping;

        public event EventHandler Stopped;
        public event UnhandledExceptionEventHandler Error;
        public event NotificationEventHandler Data;

        private CancellationTokenSource _cancellationTokenSource;
        
        private DatabaseEventListener(string connectionString, string topic)
        {
            _connectionString = connectionString;
            _topic = topic;

            var pts = new ParameterizedThreadStart(Listen);
            _thread = new Thread(pts);
        }

        public static DatabaseEventListener Create(string connectionString, string topic)
        {
            var listener = new DatabaseEventListener(connectionString, topic);
            return listener;
        }

        public void Start()
        {
            _stopping = false;
            
            _cancellationTokenSource = new CancellationTokenSource();
            var abortThreadToken = _cancellationTokenSource.Token;
            
            _thread.Start(abortThreadToken);
        }

        public void Stop(int? killAfterSeconds)
        {
            _stopping = true;
            
            if (killAfterSeconds == null)
            {
                return;
            }

            Task.Delay(TimeSpan.FromSeconds(killAfterSeconds.Value))
                .ContinueWith(_ => _cancellationTokenSource.Cancel());
        }

        private void Listen(object cancellationToken)
        {
            try
            {
                var ct = (CancellationToken) cancellationToken;

                using var conn = new NpgsqlConnection(_connectionString);
                conn.Open();

                conn.Notification += (o, e) =>
                {
                    try
                    {
                        Data?.Invoke(this, e);
                    }
                    catch (Exception ex)
                    {
                        Util.PrintException(ex);
                        Error?.Invoke(this, new UnhandledExceptionEventArgs(ex, false));
                    }
                };

                // TODO: prevent sql injection and use a parameterized function to subscribe
                using (var cmd = new NpgsqlCommand($"LISTEN {_topic};", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                while (!_stopping)
                {
                    ct.ThrowIfCancellationRequested();
                    conn.Wait(TimeSpan.FromSeconds(1));
                }

                Stopped?.Invoke(this, EventArgs.Empty);
            }
            catch (Exception ex)
            {
                Util.PrintException(ex);
                Error?.Invoke(this, new UnhandledExceptionEventArgs(ex, false));
            }
        }
    }
}
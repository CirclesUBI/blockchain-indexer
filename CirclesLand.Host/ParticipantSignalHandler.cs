/*using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Dapper;
using Newtonsoft.Json.Linq;
using Npgsql;

namespace CirclesLand.Host
{
    public class ParticipantServiceDirectory
    {
        #region Nested classes

        public class Service
        {
            public string ServiceId { get; }
            public DateTime ServiceTimeout { get; }

            public Service(string serviceId, DateTime serviceTimeout)
            {
                ServiceId = serviceId;
                ServiceTimeout = serviceTimeout;
            }
        }
        
        public class Signal
        {
            public string ServiceId { get; }
            public string Name { get; }
            public string? Value { get; }

            public Signal(string serviceId, string name, string? value)
            {
                ServiceId = serviceId;
                Name = name;
                Value = value;
            }
        }

        #endregion


        private ConcurrentDictionary<string, DateTime> _services = new ();
        private ConcurrentDictionary<string, Dictionary<string, string?>> _signals = new ();
        
        private ParticipantServiceDirectory(Dictionary<string, DateTime> services, Dictionary<string, Dictionary<string, string?>> signals)
        {
            
        }

        public static async ParticipantServiceDirectory FromDb(string connectionString)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var serviceList = await connection.QueryAsync(
                @"select service_id, timeout_at, resource_type, resource_id, resource_name, resource_value
                   from active_services");

            var servicesDict = new Dictionary<string, DateTime>();
            var signalsDict = new Dictionary<string, Dictionary<string, string?>>();
            
            foreach (var serviceDetail in serviceList)
            {
                if (!servicesDict.ContainsKey(serviceDetail.service_id))
                {
                    servicesDict.Add(serviceDetail.service_id, serviceDetail.timeout_at);
                }

                if (serviceDetail.resource_type != null && serviceDetail.resource_type == "signal")
                {
                    if (!signalsDict.TryGetValue(serviceDetail.service_id, out Dictionary<string, string?> serviceSignals))
                    {
                        serviceSignals = new Dictionary<string, string?>();
                        signalsDict.Add(serviceDetail.service_id, serviceSignals);
                    }

                    serviceSignals[serviceDetail.resource_name] = serviceDetail.resource_value;
                }
            }
        }

        public void OnServiceConnected(string serviceId, DateTime timeoutAt)
        {
        }
        public void OnServiceDisconnected(string serviceId)
        {
        }
        
        public void OnSignal(string serviceId, string name, string? value)
        {
        }
        public void OnOnlySignal(string name, string? value)
        {
        }
    }
    
    public class ParticipantSignalHandler
    {
        private readonly string _serviceId;
        private readonly string _connectionString;

        private readonly ParticipantServiceDirectory _directory = new ();
        
        public ParticipantSignalHandler(string serviceId, string connectionString)
        {
            _serviceId = serviceId;
            _connectionString = connectionString;
        }

        public void OnService(NpgsqlNotificationEventArgs e)
        {
            var now = DateTime.Now.ToUniversalTime();
            dynamic o = JObject.Parse(e.Payload);
            var timestamp = (DateTime) o.timestamp;
            var type = (string) o.type;
            var serviceId = (string) o.service_id;
            var timeoutAt = type != "disconnect" ? (DateTime?) o.timeout_at : null;
        }

        public void OnSignal(NpgsqlNotificationEventArgs e)
        {
            var now = DateTime.Now.ToUniversalTime();
            dynamic o = JObject.Parse(e.Payload);
            DateTime timestamp = o.timestamp;
            string serviceId = o.service_id;
            string name = o.name;
            string? value = o.value;
            bool isOnly = o.is_only;
        }
    }
}*/
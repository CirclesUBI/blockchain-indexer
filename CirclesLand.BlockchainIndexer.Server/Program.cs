namespace CirclesLand.BlockchainIndexer.Server
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            if (SettingsValues.DelayStartup > 0)
            {
                Console.WriteLine($"Start is delayed for {SettingsValues.DelayStartup} seconds.");
                await Task.Delay(SettingsValues.DelayStartup * 1000);
            }
            
            // This is O.K. because all dates are UTC
            AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
            
            using var host = Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseUrls(SettingsValues.WebsocketServerUrl);
                    webBuilder.UseStartup<Startup>();
                })
                .Build();

            var indexer = new Indexer();
            var cancelIndexerSource = new CancellationTokenSource();

#pragma warning disable 4014
            indexer.Run(cancelIndexerSource.Token).ContinueWith(t =>
#pragma warning restore 4014
            {
                if (t.Exception != null)
                {
                    Console.WriteLine(t.Exception.Message);
                    Console.WriteLine(t.Exception.StackTrace);
                }

                Console.WriteLine("CirclesLand.BlockchainIndexer.Indexer.Run() returned. Stopping the host..");
                try
                {
                    cancelIndexerSource.Cancel();
                }
                catch (Exception)
                {
                    Console.WriteLine("Cancellation order?: The Host ended before the Indexer");
                }
            }, cancelIndexerSource.Token);

            await host.RunAsync(cancelIndexerSource.Token);

            try
            {
                cancelIndexerSource.Cancel();
            }
            catch (Exception)
            {
                Console.WriteLine("Cancellation order?: The Indexer ended before the Host");
            }
        }
    }
}
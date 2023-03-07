using CirclesLand.BlockchainIndexer;
using CirclesLand.BlockchainIndexer.Server2;


var hostBuilder = Host.CreateDefaultBuilder(args)
    .ConfigureWebHostDefaults(webBuilder =>
    {
        webBuilder.UseStartup<Startup>();
        webBuilder.UseUrls(Settings.WebsocketServerUrl); // use the desired server URL here
    });


var app = hostBuilder.Build();

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

app.RunAsync(cancelIndexerSource.Token);

try
{
    cancelIndexerSource.Cancel();
}
catch (Exception)
{
    Console.WriteLine("Cancellation order?: The Indexer ended before the Host");
}
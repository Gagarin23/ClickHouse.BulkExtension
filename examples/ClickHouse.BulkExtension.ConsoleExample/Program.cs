using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using ClickHouse.BulkExtension;
using ClickHouse.BulkExtension.Examples;
using ClickHouse.Client.ADO;
using ClickHouse.Client.BulkExtension;

// Capacity from the application configuration
var capacity = 100_000;

// Create a bounded channel with the specified capacity
var channel = Channel.CreateBounded<YourStructType>(new BoundedChannelOptions(capacity)
{
    SingleWriter = false,
    SingleReader = true,
    FullMode = BoundedChannelFullMode.Wait
});

// Start the background bulk copy task
var backgroundTask = Task.Run(() => BackgroundBulkCopy(channel.Reader));

// Define the data download source
var someMetricsDownloadTasks = Enumerable.Range(0, 8)
    .Select(_ =>
    {
        return Task.Run(async () =>
        {
            // Replace it with a client obtained from IHttpClientFactory
            var http = new HttpClient();
            // Define your HTTP request
            var httpRequest = new HttpRequestMessage();

            // Important: Set ResponseHeadersRead to get the real network stream
            var response = await http.SendAsync(httpRequest, HttpCompletionOption.ResponseHeadersRead);
            var networkStream = await response.Content.ReadAsStreamAsync();
            // Expecting a JSON array in payload
            var asyncIterator = JsonSerializer.DeserializeAsyncEnumerable<YourStructType>(networkStream);

            await foreach (var item in asyncIterator)
            {
                await channel.Writer.WriteAsync(item);
            }
        });
    });

// Wait for all data download tasks to complete
await Task.WhenAll(someMetricsDownloadTasks);
// Signal that no more data will be written to the channel
channel.Writer.Complete();
// Wait for the background bulk copy task to finish
await backgroundTask;
return;

static async Task BackgroundBulkCopy(ChannelReader<YourStructType> reader)
{
    try
    {
        var connectionString = "Your ClickHouse connection string";
        await using var connection = new ClickHouseConnection(connectionString);

        // Option 1: Use BulkCopyAsync extension method
        while (await reader.WaitToReadAsync())
        {
            var asyncIteratedBatch = GetBatchAsync(reader);

            await connection.BulkCopyAsync(
                tableName: "your_table",
                columns: ["Column1", "Column2", "Column3"],
                source: asyncIteratedBatch,
                // Avoid using compression if you are on a local network
                useCompression: false,
                cancellationToken: CancellationToken.None);
        }

        // Option 2: Use ClickHouseAsyncCopy with PostStreamAsync
        // We can save just a bit of memory on creating a ClickHouseAsyncCopy object
        var copy = new ClickHouseAsyncCopy<YourStructType>("your_table", ["Column1", "Column2", "Column3"]);
        while (await reader.WaitToReadAsync())
        {
            var asyncIteratedBatch = GetBatchAsync(reader);
            var streamWriteFunction = copy.GetStreamWriteCallBack(asyncIteratedBatch, useCompression: false);
            await connection.PostStreamAsync(null, streamWriteFunction, false, CancellationToken.None);
        }

    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"An error occurred during bulk copy: {ex.Message}");
        throw;
    }
}

static async IAsyncEnumerable<YourStructType> GetBatchAsync(ChannelReader<YourStructType> reader)
{
    // Get the batch size from the application configuration
    const int maxBatchSize = 100_000;
    const int minBatchSize = 10_000;
    // Get the maximum batch duration from the application configuration
    var maxBatchDuration = TimeSpan.FromSeconds(1); // Adjust the duration as needed
    var stopwatch = Stopwatch.StartNew();

    var count = 0;
    await foreach(var item in reader.ReadAllAsync())
    {
        if ((count == maxBatchSize || stopwatch.Elapsed >= maxBatchDuration) && count > minBatchSize)
        {
            break;
        }

        yield return item;
        count++;
    }
}

// It is important to use a struct as a buffer for data units,
// because a struct does not incur additional memory allocation on the heap.
// While large structs may increase CPU copy operations,
// we avoid garbage collection and memory fragmentation.
// Therefore, using structs in this way is more scalable.
namespace ClickHouse.BulkExtension.Examples
{
    public struct YourStructType
    {
        // Define your struct fields or properties here
    }
}
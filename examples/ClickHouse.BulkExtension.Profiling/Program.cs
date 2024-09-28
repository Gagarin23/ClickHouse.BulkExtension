using System.Diagnostics;
using System.Threading.Channels;
using ClickHouse.BulkExtension;
using ClickHouse.BulkExtension.Annotation;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;

var capacity = 1_000_000;

var channel = Channel.CreateBounded<YourStructType>(new BoundedChannelOptions(capacity)
{
    SingleWriter = true,
    SingleReader = true,
    FullMode = BoundedChannelFullMode.Wait
});

_ = Task.Run(() => BackgroundBulkCopy(channel.Reader));

var backgroundTask = Task.Run(() =>
{
    while (true)
    {
        channel.Writer.TryWrite(new YourStructType
        {
            Column1 = DateTime.UtcNow,
            Column2 = 1.0,
            Column3 = 2.0
        });
    }
});

await backgroundTask;

return;

static async Task BackgroundBulkCopy(ChannelReader<YourStructType> reader)
{
    try
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");
        await using var connection = new ClickHouseConnection(connectionString);

        await connection.ExecuteStatementAsync(@"
CREATE TABLE IF NOT EXISTS virtual_test_table 
(
    Column1 datetime64(6), -- 6 - Microsecond
    Column2 Float64,
    Column3 Float64
) 
    ENGINE = Null");

        var copy = new ClickHouseAsyncCopy<YourStructType>("virtual_test_table", ["Column1", "Column2", "Column3"]);
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
    const int maxBatchSize = 1_000_000;
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

public struct YourStructType
{
    [ClickHouseColumn(DateTimePrecision = DateTimePrecision.Microsecond)]
    public DateTime Column1 { get; set; }
    public double Column2 { get; set; }
    public double Column3 { get; set; }
}
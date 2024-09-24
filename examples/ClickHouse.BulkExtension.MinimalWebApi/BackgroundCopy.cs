using System.Diagnostics;
using System.Threading.Channels;
using ClickHouse.BulkExtension;
using ClickHouse.BulkExtension.Annotation;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using Microsoft.Extensions.Options;

namespace ClickHouse.Client.BulkExtension.MinimalWebApi;

// It is important to use a struct as a buffer for data units
// because a struct does not incur additional memory allocation on the heap.
// While large structs may increase CPU copy operations,
// we avoid garbage collection and memory fragmentation.
// Therefore, using structs in this way is more scalable.
public struct YourStructType
{
    [ClickHouseColumn(DateTimePrecision = DateTimePrecision.Microsecond)]
    public DateTime Column1 { get; set; }
    public double Column2 { get; set; }
    public double Column3 { get; set; }
}

public class BackgroundCopy : BackgroundService
{
    private readonly ClickHouseConnection _connection;
    private readonly IOptionsMonitor<AppOptions> _monitor;
    private readonly ChannelReader<YourStructType> _reader;

    private int _itemCounter = 0;

    public BackgroundCopy(ChannelHolder channelHolder, ClickHouseConnection connection, IOptionsMonitor<AppOptions> monitor)
    {
        _reader = channelHolder.Reader;
        _connection = connection;
        _monitor = monitor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _connection.ExecuteStatementAsync(@"
CREATE TABLE IF NOT EXISTS test_table 
(
    Column1 datetime64(6), -- 6 - Microsecond
    Column2 Float64,
    Column3 Float64
) 
    ENGINE = MergeTree()
    PARTITION BY (Column1)
    ORDER BY (Column1)");
        await BackgroundBulkCopy(stoppingToken);
    }

    async Task BackgroundBulkCopy(CancellationToken cancellationToken)
    {
        try
        {
            var copy = new ClickHouseAsyncCopy<YourStructType>("test_table", ["Column1", "Column2", "Column3"]);
            while (await _reader.WaitToReadAsync(cancellationToken))
            {
                var asyncIteratedBatch = GetBatchAsync(_reader, cancellationToken);
                var streamWriteFunction = copy.GetStreamWriteCallBack(asyncIteratedBatch, useCompression: false);
                await _connection.PostStreamAsync(null, streamWriteFunction, false, cancellationToken);
                await Console.Out.WriteLineAsync($"Batch of {_itemCounter} items has been copied.");
                _itemCounter = 0;
            }
        }
        catch (Exception ex)
        {
            await Console.Error.WriteLineAsync($"An error occurred during bulk copy: {ex.Message}");
            throw;
        }
    }

    private async IAsyncEnumerable<YourStructType> GetBatchAsync(ChannelReader<YourStructType> reader, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        var current = _monitor.CurrentValue;
        var maxBatchSize = current.MaxBatchSize;
        var maxBatchDuration = TimeSpan.FromSeconds(current.MaxBatchDurationInSeconds);

        await foreach(var item in reader.ReadAllAsync(cancellationToken))
        {
            if ((_itemCounter == maxBatchSize || stopwatch.Elapsed >= maxBatchDuration) && _itemCounter > 0)
            {
                break;
            }

            yield return item;
            _itemCounter++;
        }
    }
}
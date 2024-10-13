using System.Diagnostics;
using System.Threading.Channels;
using ClickHouse.BulkExtension.Annotation;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;

namespace ClickHouse.BulkExtension.Profiling;

public static class ChClientProfiling
{
    private const int Capacity = 1_000_000;

    private static readonly Channel<object[]> Channel = System.Threading.Channels.Channel.CreateBounded<object[]>(new BoundedChannelOptions(Capacity)
    {
        SingleWriter = true,
        SingleReader = true,
        FullMode = BoundedChannelFullMode.Wait
    });

    public static async Task RunAsync()
    {
        _ = Task.Run(() => BackgroundBulkCopy(Channel.Reader));

        var backgroundTask = Task.Run(() =>
        {
            while (true)
            {
                Channel.Writer.TryWrite(new object[]
                {
                    DateTime.UtcNow,
                    1.0,
                    2.0
                });
            }
        });

        await backgroundTask;
    }

    static async Task BackgroundBulkCopy(ChannelReader<object[]> reader)
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

            const int batchSize = 10_000;
            var copy = new ClickHouseBulkCopy(connection)
            {
                DestinationTableName = "virtual_test_table",
                ColumnNames = new[] { "Column1", "Column2", "Column3" },
                BatchSize = batchSize,
                MaxDegreeOfParallelism = Environment.ProcessorCount
            };
            await copy.InitAsync();
            while (await reader.WaitToReadAsync())
            {
                var batch = await GetBatchAsync(reader, batchSize);
                await copy.WriteToServerAsync(batch);
            }

        }
        catch (Exception ex)
        {
            await Console.Error.WriteLineAsync($"An error occurred during bulk copy: {ex.Message}");
            throw;
        }
    }

    static async ValueTask<List<object[]>> GetBatchAsync(ChannelReader<object[]> reader, int batchSize)
    {
        var count = 0;
        var buffer = new List<object[]>(batchSize);
        await foreach(var item in reader.ReadAllAsync())
        {
            buffer.Add(item);
            count++;

            if (batchSize == count)
            {
                break;
            }
        }

        return buffer;
    }
}
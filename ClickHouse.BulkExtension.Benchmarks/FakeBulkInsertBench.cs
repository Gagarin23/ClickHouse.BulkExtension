using System.Reflection;
using BenchmarkDotNet.Attributes;
using ClickHouse.BulkExtension.Annotation;

namespace ClickHouse.BulkExtension.Benchmarks;

/*

| Method                     | Count   | Mean         | Error       | StdDev      |
|--------------------------- |-------- |-------------:|------------:|------------:|
| NewBulkInsertInt64         | 10000   |     268.2 us |     3.63 us |     3.03 us |
| NewGenericBulkInsertEntity | 10000   |   6,381.5 us |   119.47 us |   122.69 us |
| NewAsyncBulkInsertEntity   | 10000   |   6,078.7 us |   119.30 us |   111.59 us |
| NewBulkInsertInt64         | 100000  |   2,934.1 us |    57.90 us |    71.10 us |
| NewGenericBulkInsertEntity | 100000  |  60,526.7 us |   530.97 us |   470.69 us |
| NewAsyncBulkInsertEntity   | 100000  |  58,747.7 us |   542.39 us |   423.46 us |
| NewBulkInsertInt64         | 300000  |   8,583.1 us |   168.59 us |   219.21 us |
| NewGenericBulkInsertEntity | 300000  | 167,346.8 us |   476.15 us |   371.75 us |
| NewAsyncBulkInsertEntity   | 300000  | 173,255.3 us | 1,257.96 us | 1,176.70 us |
| NewBulkInsertInt64         | 1000000 |  25,168.8 us |   473.31 us |   442.73 us |
| NewGenericBulkInsertEntity | 1000000 | 566,928.2 us | 3,486.72 us | 3,090.88 us |
| NewAsyncBulkInsertEntity   | 1000000 | 596,145.3 us | 4,116.37 us | 3,437.36 us |

 */

[GcServer(true)]
//[MemoryDiagnoser] because we have allocations in fake stream - ReadAsStreamAsync (MemoryStream)
public class FakeBulkInsertBench
{
    private ClickHouseCopy<Int64Wrapper> _newBulkCopyInt;
    private ClickHouseCopy<PrimitiveTableType> _newGenericBulkCopyEntity;
    private ClickHouseAsyncCopy<PrimitiveTableType> _newAsyncBulkCopyEntity;

    private readonly string[] _columns = typeof(PrimitiveTableType)
        .GetProperties()
        .Select(x => x.GetCustomAttribute<ClickHouseColumnAttribute>()?.Name ?? x.Name)
        .ToArray();

    private Memory<byte> _buffer;


    [Params(10_000, 100_000, 300_000, 1_000_000)]
    public int Count { get; set; }

    private IEnumerable<Int64Wrapper> IntRows
    {
        get
        {
            for (int i = 0; i < Count; i++)
            {
                yield return new Int64Wrapper { Value = i };
            }
        }
    }

    private const string StringForNoAlloc = "String";

    private PrimitiveTableType GetEntity(int i) => new PrimitiveTableType()
    {
        GuidColumn = Guid.NewGuid(),
        BooleanColumn = i % 2 == 0,
        StringColumn = StringForNoAlloc,
        DecimalColumn = i + 0.1m,
        DoubleColumn = i + 0.2,
        FloatColumn = i + 0.3f,
        IntColumn = i + 3,
        LongColumn = i,
        ShortColumn = (short)i,
        DateTimeColumn = DateTime.Now.AddMinutes(i),
        ValueTupleColumn = (StringForNoAlloc, i, i)
    };

    private IEnumerable<PrimitiveTableType> PrimitiveTableTypeRows
    {
        get
        {
            for (int i = 0; i < Count; i++)
            {
                yield return GetEntity(i);
            }
        }
    }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    private async IAsyncEnumerable<PrimitiveTableType> GetAsyncPrimitiveTableTypeRows()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
    {
        for (int i = 0; i < Count; i++)
        {
            yield return GetEntity(i);
        }
    }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _newBulkCopyInt = new ClickHouseCopy<Int64Wrapper>("benchmark_bulk_insert_int64", ["Value"]);
        _newGenericBulkCopyEntity = new ClickHouseCopy<PrimitiveTableType>("benchmark_bulk_insert_entity", _columns);
        _newAsyncBulkCopyEntity = new ClickHouseAsyncCopy<PrimitiveTableType>("benchmark_bulk_insert_entity", _columns);

        _buffer = new Memory<byte>(new byte[4096]);
    }

    [Benchmark]
    public async Task NewBulkInsertInt64()
    {
        var streamContent = _newBulkCopyInt.GetStreamContent(IntRows, false);
        var outputStream = await streamContent.ReadAsStreamAsync();
        var read = -1;
        while (read != 0)
        {
            read = await outputStream.ReadAtLeastAsync(_buffer, _buffer.Length >> 1, throwOnEndOfStream: false);
        }
    }

    [Benchmark]
    public async Task NewGenericBulkInsertEntity()
    {
        var streamContent = _newGenericBulkCopyEntity.GetStreamContent(PrimitiveTableTypeRows, false);
        var outputStream = await streamContent.ReadAsStreamAsync();
        var read = -1;
        while (read != 0)
        {
            read = await outputStream.ReadAtLeastAsync(_buffer, _buffer.Length >> 1, throwOnEndOfStream: false);
        }
    }

    [Benchmark]
    public async Task NewAsyncBulkInsertEntity()
    {
        var streamContent = _newAsyncBulkCopyEntity.GetStreamContent(GetAsyncPrimitiveTableTypeRows(), false);
        var outputStream = await streamContent.ReadAsStreamAsync();
        var read = -1;
        while (read != 0)
        {
            read = await outputStream.ReadAtLeastAsync(_buffer, _buffer.Length >> 1, throwOnEndOfStream: false);
        }
    }
}
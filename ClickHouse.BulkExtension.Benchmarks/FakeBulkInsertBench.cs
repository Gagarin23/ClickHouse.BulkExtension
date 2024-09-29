using System.Reflection;
using BenchmarkDotNet.Attributes;
using ClickHouse.BulkExtension.Annotation;

namespace ClickHouse.BulkExtension.Benchmarks;

/*

| Method                     | Count   | Mean         | Error       | StdDev      |
|--------------------------- |-------- |-------------:|------------:|------------:|
| NewBulkInsertInt64         | 10000   |     297.2 us |     5.93 us |    14.21 us |
| NewGenericBulkInsertEntity | 10000   |   6,438.9 us |   126.51 us |   173.16 us |
| NewAsyncBulkInsertEntity   | 10000   |   6,678.2 us |   130.85 us |   170.14 us |
| NewBulkInsertInt64         | 100000  |   3,283.9 us |    64.72 us |    77.05 us |
| NewGenericBulkInsertEntity | 100000  |  59,262.2 us |   748.88 us |   625.35 us |
| NewAsyncBulkInsertEntity   | 100000  |  62,320.4 us | 1,131.91 us | 1,058.79 us |
| NewBulkInsertInt64         | 300000  |   9,439.2 us |   188.74 us |   293.85 us |
| NewGenericBulkInsertEntity | 300000  | 173,578.8 us |   847.73 us |   751.49 us |
| NewAsyncBulkInsertEntity   | 300000  | 178,234.9 us |   972.83 us |   812.36 us |
| NewBulkInsertInt64         | 1000000 |  25,754.8 us |   434.61 us |   406.54 us |
| NewGenericBulkInsertEntity | 1000000 | 577,101.2 us | 3,267.39 us | 2,896.45 us |
| NewAsyncBulkInsertEntity   | 1000000 | 601,210.6 us | 6,959.46 us | 6,169.38 us |

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
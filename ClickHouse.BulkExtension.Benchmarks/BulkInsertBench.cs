using System.Reflection;
using BenchmarkDotNet.Attributes;
using ClickHouse.BulkExtension.Annotation;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;

namespace ClickHouse.BulkExtension.Benchmarks;

/*

BenchmarkDotNet v0.14.0, Windows 11 (10.0.22631.4169/23H2/2023Update/SunValley3)
AMD Ryzen 7 5800X, 1 CPU, 16 logical and 8 physical cores
.NET SDK 8.0.108
  [Host]     : .NET 8.0.8 (8.0.824.36612), X64 RyuJIT AVX2
  Job-WWETBA : .NET 8.0.8 (8.0.824.36612), X64 RyuJIT AVX2

Server=True

| Method                     | Count   | Mean        | Error     | StdDev    | Allocated    |
|--------------------------- |-------- |------------:|----------:|----------:|-------------:|
| BulkInsertInt64            | 10000   |    50.38 ms |  0.934 ms |  0.828 ms |    949.69 KB |
| NewBulkInsertInt64         | 10000   |    51.95 ms |  1.009 ms |  1.541 ms |     10.18 KB |
| BulkInsertEntity           | 10000   |    31.05 ms |  1.643 ms |  4.713 ms |   7211.42 KB |
| NewBulkInsertEntity        | 10000   |    60.53 ms |  0.969 ms |  0.907 ms |     10.85 KB |
| NewGenericBulkInsertEntity | 10000   |    60.50 ms |  0.992 ms |  0.928 ms |     10.95 KB |
| NewAsyncBulkInsertEntity   | 10000   |    61.03 ms |  1.203 ms |  1.182 ms |     10.98 KB |
| BulkInsertInt64            | 100000  |    46.73 ms |  1.114 ms |  3.285 ms |   6767.41 KB |
| NewBulkInsertInt64         | 100000  |    50.13 ms |  0.467 ms |  0.390 ms |     10.18 KB |
| BulkInsertEntity           | 100000  |   195.88 ms |  2.925 ms |  2.443 ms |  69989.35 KB |
| NewBulkInsertEntity        | 100000  |   199.56 ms |  0.663 ms |  0.517 ms |     11.25 KB |
| NewGenericBulkInsertEntity | 100000  |   202.97 ms |  3.573 ms |  3.342 ms |      11.4 KB |
| NewAsyncBulkInsertEntity   | 100000  |   203.63 ms |  2.501 ms |  2.217 ms |     11.55 KB |
| BulkInsertInt64            | 300000  |    38.08 ms |  1.163 ms |  3.411 ms |  20777.15 KB |
| NewBulkInsertInt64         | 300000  |    60.05 ms |  0.526 ms |  0.466 ms |     10.24 KB |
| BulkInsertEntity           | 300000  |   582.16 ms |  3.772 ms |  3.150 ms | 210602.02 KB |
| NewBulkInsertEntity        | 300000  |   518.25 ms |  7.639 ms |  7.145 ms |     12.77 KB |
| NewGenericBulkInsertEntity | 300000  |   516.85 ms | 10.327 ms | 10.143 ms |     11.17 KB |
| NewAsyncBulkInsertEntity   | 300000  |   526.12 ms |  6.693 ms |  6.261 ms |     13.41 KB |
| BulkInsertInt64            | 1000000 |   104.73 ms |  2.023 ms |  2.559 ms |  63155.63 KB |
| NewBulkInsertInt64         | 1000000 |   100.03 ms |  0.727 ms |  0.607 ms |     10.49 KB |
| BulkInsertEntity           | 1000000 | 2,007.25 ms | 30.629 ms | 28.651 ms | 696371.72 KB |
| NewBulkInsertEntity        | 1000000 | 1,599.89 ms |  5.920 ms |  5.538 ms |     12.23 KB |
| NewGenericBulkInsertEntity | 1000000 | 1,602.88 ms | 10.287 ms |  9.622 ms |     13.41 KB |
| NewAsyncBulkInsertEntity   | 1000000 | 1,618.87 ms |  7.156 ms |  6.344 ms |     13.45 KB |

 */

[GcServer(true)]
[MemoryDiagnoser]
public class BulkInsertBench
{
    private ClickHouseConnection _connection;

    private ClickHouseBulkCopy _bulkCopyInt;
    private ClickHouseBulkCopy _bulkCopyEntity;

    private ClickHouseCopy _newBulkCopyInt;
    private ClickHouseCopy _newBulkCopyEntity;
    private ClickHouseCopy<PrimitiveTableType> _newGenericBulkCopyEntity;
    private ClickHouseAsyncCopy<PrimitiveTableType> _newAsyncBulkCopyEntity;

    private readonly string[] _columns = typeof(PrimitiveTableType)
        .GetProperties()
        .Select(x => x.GetCustomAttribute<ClickHouseColumnAttribute>()?.Name ?? x.Name)
        .ToArray();



    [Params(10_000, 100_000, 300_000, 1_000_000)]
    public int Count { get; set; }

    private IEnumerable<object[]> ObjectIntRows
    {
        get
        {
            for (int i = 0; i < Count; i++)
            {
                yield return new object[] { i };
            }
        }
    }

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

    private IEnumerable<object[]> ObjectPrimitiveTableTypeRows
    {
        get
        {
            for (int i = 0; i < Count; i++)
            {
                yield return new object[]
                {
                    Guid.NewGuid(),
                    i % 2 == 0,
                    StringForNoAlloc,
                    i + 0.1m,
                    i + 0.2,
                    i + 0.3f,
                    i + 3,
                    i,
                    (short)i,
                    DateTime.Now.AddMinutes(i),
                    (StringForNoAlloc, i, i)
                };
            }
        }
    }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");
        _connection = new ClickHouseConnection(connectionString);

        await _connection.ExecuteStatementAsync("DROP TABLE IF EXISTS benchmark_bulk_insert_int64");
        await _connection.ExecuteStatementAsync("CREATE TABLE benchmark_bulk_insert_int64 (Value Int64) ENGINE Null");

        _bulkCopyInt = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "benchmark_bulk_insert_int64",
            BatchSize = Count,
            MaxDegreeOfParallelism = 1,
            ColumnNames = ["Value"]
        };
        await _bulkCopyInt.InitAsync();

        _newBulkCopyInt = new ClickHouseCopy("benchmark_bulk_insert_int64", ["Value"], IntRows);

        await _connection.ExecuteStatementAsync("DROP TABLE IF EXISTS benchmark_bulk_insert_entity");
        await _connection.ExecuteStatementAsync(@"
CREATE TABLE benchmark_bulk_insert_entity 
(
    Qwerty UUID,
    BooleanColumn bool,
    StringColumn nvarchar,
    DecimalColumn decimal(18, 6),
    FloatColumn Float32,
    DoubleColumn Float64,
    ShortColumn Int16,
    IntColumn Int32,
    LongColumn Int64,
    DateTimeColumn datetime64(6),
    ValueTupleColumn Tuple(s String, i32 Int32, i64 Int64)
) 
    ENGINE Null");

        _bulkCopyEntity = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "benchmark_bulk_insert_entity",
            BatchSize = Count,
            MaxDegreeOfParallelism = 1,
            ColumnNames = _columns
        };
        await _bulkCopyEntity.InitAsync();

        _newBulkCopyEntity = new ClickHouseCopy("benchmark_bulk_insert_entity", _columns, PrimitiveTableTypeRows);
        _newGenericBulkCopyEntity = new ClickHouseCopy<PrimitiveTableType>("benchmark_bulk_insert_entity", _columns);
        _newAsyncBulkCopyEntity = new ClickHouseAsyncCopy<PrimitiveTableType>("benchmark_bulk_insert_entity", _columns);
    }

    //[Benchmark]
    public async Task BulkInsertInt64()
    {
        await _bulkCopyInt.WriteToServerAsync(ObjectIntRows);
    }

    //[Benchmark]
    public async Task NewBulkInsertInt64()
    {
        await _connection.PostStreamAsync(null, _newBulkCopyInt.GetStreamWriteCallBack(true), true, CancellationToken.None);
    }

    //[Benchmark]
    public async Task BulkInsertEntity()
    {
        await _bulkCopyEntity.WriteToServerAsync(ObjectPrimitiveTableTypeRows);
    }

    [Benchmark]
    public async Task NewBulkInsertEntity()
    {
        await _connection.PostStreamAsync(null, _newBulkCopyEntity.GetStreamWriteCallBack(false), false, CancellationToken.None);
    }

    //[Benchmark]
    public async Task NewGenericBulkInsertEntity()
    {
        await _connection.PostStreamAsync(null, _newGenericBulkCopyEntity.GetStreamWriteCallBack(PrimitiveTableTypeRows, true), true, CancellationToken.None);
    }

    //[Benchmark]
    public async Task NewAsyncBulkInsertEntity()
    {
        await _connection.PostStreamAsync(null, _newAsyncBulkCopyEntity.GetStreamWriteCallBack(GetAsyncPrimitiveTableTypeRows(), true), true, CancellationToken.None);
    }
}

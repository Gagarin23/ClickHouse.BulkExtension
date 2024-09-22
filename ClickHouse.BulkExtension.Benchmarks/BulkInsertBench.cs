using System.Reflection;
using BenchmarkDotNet.Attributes;
using ClickHouse.BulkExtension.Annotation;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;

namespace ClickHouse.BulkExtension.Benchmarks;

/*

| Method                   | Count   | Mean        | Error     | StdDev    | Allocated    |
|------------------------- |-------- |------------:|----------:|----------:|-------------:|
| BulkInsertInt64          | 10000   |    50.91 ms |  0.698 ms |  0.583 ms |    949.67 KB |
| NewBulkInsertInt64       | 10000   |    51.94 ms |  1.010 ms |  1.240 ms |     84.52 KB |
| BulkInsertEntity         | 10000   |    35.98 ms |  2.205 ms |  6.467 ms |   7133.22 KB |
| NewBulkInsertEntity      | 10000   |    60.88 ms |  1.044 ms |  0.977 ms |     11.11 KB |
| NewAsyncBulkInsertEntity | 10000   |    60.17 ms |  0.670 ms |  0.594 ms |     10.92 KB |
| BulkInsertInt64          | 100000  |    44.02 ms |  1.431 ms |  4.219 ms |   6767.49 KB |
| NewBulkInsertInt64       | 100000  |    51.20 ms |  0.877 ms |  0.861 ms |     80.91 KB |
| BulkInsertEntity         | 100000  |   196.51 ms |  3.248 ms |  3.038 ms |  69206.82 KB |
| NewBulkInsertEntity      | 100000  |   195.85 ms |  3.527 ms |  3.299 ms |      11.9 KB |
| NewAsyncBulkInsertEntity | 100000  |   198.49 ms |  2.355 ms |  2.202 ms |      11.6 KB |
| BulkInsertInt64          | 300000  |    36.55 ms |  0.978 ms |  2.837 ms |  20777.17 KB |
| NewBulkInsertInt64       | 300000  |    61.92 ms |  1.006 ms |  0.941 ms |     83.67 KB |
| BulkInsertEntity         | 300000  |   582.59 ms | 11.555 ms | 14.613 ms | 208255.91 KB |
| NewBulkInsertEntity      | 300000  |   501.29 ms |  3.488 ms |  3.262 ms |      14.1 KB |
| NewAsyncBulkInsertEntity | 300000  |   504.80 ms |  6.705 ms |  6.272 ms |     12.54 KB |
| BulkInsertInt64          | 1000000 |   104.55 ms |  1.811 ms |  1.694 ms |  63155.59 KB |
| NewBulkInsertInt64       | 1000000 |   100.47 ms |  1.552 ms |  1.452 ms |     80.37 KB |
| BulkInsertEntity         | 1000000 | 1,913.12 ms | 36.577 ms | 30.543 ms | 688518.85 KB |
| NewBulkInsertEntity      | 1000000 | 1,537.88 ms | 27.933 ms | 24.762 ms |     13.73 KB |
| NewAsyncBulkInsertEntity | 1000000 | 1,553.87 ms | 11.480 ms | 10.177 ms |     12.21 KB |

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

    [Benchmark]
    public async Task BulkInsertInt64()
    {
        await _bulkCopyInt.WriteToServerAsync(ObjectIntRows);
    }

    [Benchmark]
    public async Task NewBulkInsertInt64()
    {
        await _connection.PostStreamAsync(null, _newBulkCopyInt.GetStreamWriteCallBack(true), true, CancellationToken.None);
    }

    [Benchmark]
    public async Task BulkInsertEntity()
    {
        await _bulkCopyEntity.WriteToServerAsync(ObjectPrimitiveTableTypeRows);
    }

    [Benchmark]
    public async Task NewBulkInsertEntity()
    {
        await _connection.PostStreamAsync(null, _newBulkCopyEntity.GetStreamWriteCallBack(true), true, CancellationToken.None);
    }

    [Benchmark]
    public async Task NewGenericBulkInsertEntity()
    {
        await _connection.PostStreamAsync(null, _newGenericBulkCopyEntity.GetStreamWriteCallBack(PrimitiveTableTypeRows, true), true, CancellationToken.None);
    }

    [Benchmark]
    public async Task NewAsyncBulkInsertEntity()
    {
        await _connection.PostStreamAsync(null, _newAsyncBulkCopyEntity.GetStreamWriteCallBack(GetAsyncPrimitiveTableTypeRows(), true), true, CancellationToken.None);
    }
}

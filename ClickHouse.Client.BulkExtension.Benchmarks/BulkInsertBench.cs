using System.Numerics;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using ClickHouse.Client.ADO;
using ClickHouse.Client.BulkExtension.Annotation;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.BulkExtension.Benchmarks;

/*

| Method                   | Count  | Mean      | Error    | StdDev   | Allocated    |
|------------------------- |------- |----------:|---------:|---------:|-------------:|
| BulkInsertInt32          | 10000  |  50.44 ms | 0.874 ms | 0.775 ms |    949.64 KB |
| NewBulkInsertInt32       | 10000  |  50.01 ms | 0.545 ms | 0.483 ms |     80.43 KB |
| BulkInsertEntity         | 10000  |  36.07 ms | 2.005 ms | 5.913 ms |   7133.22 KB |
| NewBulkInsertEntity      | 10000  |  60.43 ms | 0.729 ms | 0.682 ms |     11.23 KB |
| NewAsyncBulkInsertEntity | 10000  |  60.40 ms | 0.985 ms | 0.921 ms |     10.95 KB |
| BulkInsertInt32          | 100000 |  44.22 ms | 1.008 ms | 2.971 ms |   6767.41 KB |
| NewBulkInsertInt32       | 100000 |  51.79 ms | 1.008 ms | 1.079 ms |     79.36 KB |
| BulkInsertEntity         | 100000 | 193.74 ms | 1.860 ms | 1.452 ms |  69207.14 KB |
| NewBulkInsertEntity      | 100000 | 192.16 ms | 2.948 ms | 2.758 ms |     11.91 KB |
| NewAsyncBulkInsertEntity | 100000 | 199.41 ms | 0.433 ms | 0.361 ms |     11.27 KB |
| BulkInsertInt32          | 300000 |  36.90 ms | 1.145 ms | 3.323 ms |  20777.09 KB |
| NewBulkInsertInt32       | 300000 |  61.88 ms | 1.203 ms | 1.181 ms |     79.92 KB |
| BulkInsertEntity         | 300000 | 572.20 ms | 7.518 ms | 7.032 ms | 208255.15 KB |
| NewBulkInsertEntity      | 300000 | 492.94 ms | 5.700 ms | 5.331 ms |     12.46 KB |
| NewAsyncBulkInsertEntity | 300000 | 502.44 ms | 6.539 ms | 6.117 ms |     10.87 KB |

 */

[GcServer(true)]
[MemoryDiagnoser]
public class BulkInsertBench
{
    private ClickHouseConnection _connection;
    private ClickHouseBulkCopy _bulkCopyInt;
    private ClickHouseBulkCopy _bulkCopyEntity;

    private readonly string[] _sortedColumns = typeof(PrimitiveTableType)
        .GetProperties()
        .Select(x => x.GetCustomAttribute<ClickHouseColumnAttribute>()?.Name ?? x.Name)
        .ToArray();


    [Params(10_000, 100_000, 300_000, 1_000_000)]
    public int Count { get; set; } = 100_000;

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
            ColumnNames = new[] { "Value" }
        };
        await _bulkCopyInt.InitAsync();

        await _connection.ExecuteStatementAsync("DROP TABLE IF EXISTS benchmark_bulk_insert_entity");
        await _connection.ExecuteStatementAsync(@"
CREATE TABLE benchmark_bulk_insert_entity 
(
    Qwerty UUID,
    BooleanColumn bool,
    StringColumn nvarchar,
    DecimalColumn decimal(18, 6),
    DoubleColumn Float64,
    FloatColumn Float32,
    IntColumn Int32,
    LongColumn Int64,
    ShortColumn Int16,
    BigInteger128Column Int128,
    BigInteger256Column Int256,
    BigIntegerU128Column UInt128,
    BigIntegerU256Column UInt256,
    DateTimeColumn datetime64,
    ArrayColumn Array(Int32),
    MapColumn Map(Int64, String),
    TupleColumn Tuple(s String, i32 Int32, i64 Int64),
    ValueTupleColumn Tuple(s String, i32 Int32, i64 Int64)
) 
    ENGINE Null");

        _bulkCopyEntity = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "benchmark_bulk_insert_entity",
            BatchSize = Count,
            MaxDegreeOfParallelism = 1,
            ColumnNames = _sortedColumns
        };
        await _bulkCopyEntity.InitAsync();
    }

    [Benchmark]
    public async Task BulkInsertInt32()
    {
        await _bulkCopyInt.WriteToServerAsync(ObjectIntRows);
    }

    [Benchmark]
    public async Task NewBulkInsertInt32()
    {
        var reader = new ClickHouseBulkReader(IntRows, new[] {"Value"}, "benchmark_bulk_insert_int64", true);
        await _connection.PostStreamAsync(null, reader.GetStreamWriteCallBack(), true, CancellationToken.None);
    }

    [Benchmark]
    public async Task BulkInsertEntity()
    {
        await _bulkCopyEntity.WriteToServerAsync(ObjectPrimitiveTableTypeRows);
    }

    [Benchmark]
    public async Task NewBulkInsertEntity()
    {
        var reader = new ClickHouseBulkReader(PrimitiveTableTypeRows, _sortedColumns, "benchmark_bulk_insert_entity", true);
        await _connection.PostStreamAsync(null, reader.GetStreamWriteCallBack(), true, CancellationToken.None);
    }

    [Benchmark]
    public async Task NewAsyncBulkInsertEntity()
    {
        var reader = new ClickHouseBulkAsyncReader<PrimitiveTableType>(GetAsyncPrimitiveTableTypeRows(), _sortedColumns, "benchmark_bulk_insert_entity", true);
        await _connection.PostStreamAsync(null, reader.GetStreamWriteCallBack(), true, CancellationToken.None);
    }
}

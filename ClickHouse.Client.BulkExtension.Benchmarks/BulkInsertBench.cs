using System.Numerics;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using ClickHouse.Client.ADO;
using ClickHouse.Client.BulkExtension.Annotation;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.BulkExtension.Benchmarks;

/*

| Method                   | Count  | Mean      | Error     | StdDev    | Allocated   |
|------------------------- |------- |----------:|----------:|----------:|------------:|
| BulkInsertInt32          | 10000  |  51.01 ms |  0.968 ms |  1.115 ms |   950.92 KB |
| NewBulkInsertInt32       | 10000  |  51.08 ms |  1.018 ms |  1.251 ms |    28.05 KB |
| BulkInsertEntity         | 10000  |  34.24 ms |  2.021 ms |  5.928 ms |  8541.46 KB |
| NewBulkInsertEntity      | 10000  |  70.46 ms |  0.921 ms |  0.861 ms |    15.21 KB |
| NewAsyncBulkInsertEntity | 10000  |  71.46 ms |  1.381 ms |  1.356 ms |    15.13 KB |
| BulkInsertInt32          | 100000 |  45.67 ms |  1.247 ms |  3.678 ms |  6768.71 KB |
| NewBulkInsertInt32       | 100000 |  60.68 ms |  0.753 ms |  0.668 ms |    32.82 KB |
| BulkInsertEntity         | 100000 | 210.84 ms |  3.448 ms |  3.225 ms | 83271.48 KB |
| NewBulkInsertEntity      | 100000 | 297.34 ms |  5.852 ms | 10.700 ms |     16.3 KB |
| NewAsyncBulkInsertEntity | 100000 | 289.52 ms |  3.937 ms |  3.683 ms |    19.11 KB |
| BulkInsertInt32          | 300000 |  38.46 ms |  1.061 ms |  3.044 ms | 20778.44 KB |
| NewBulkInsertInt32       | 300000 |  80.68 ms |  0.825 ms |  0.689 ms |    28.69 KB |
| BulkInsertEntity         | 300000 | 613.42 ms |  4.486 ms |  4.196 ms | 250446.5 KB |
| NewBulkInsertEntity      | 300000 | 769.97 ms |  8.355 ms |  7.815 ms |    19.09 KB |
| NewAsyncBulkInsertEntity | 300000 | 791.62 ms | 10.304 ms |  9.638 ms |    20.46 KB |

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


    [Params(10_000, 100_000, 300_000 /*, 1_000_000*/)]
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

        /*await _connection.ExecuteStatementAsync("DROP TABLE IF EXISTS benchmark_bulk_insert_int64");
        await _connection.ExecuteStatementAsync("CREATE TABLE benchmark_bulk_insert_int64 (Value Int64) ENGINE Null");

        _bulkCopyInt = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "benchmark_bulk_insert_int64",
            BatchSize = Count,
            MaxDegreeOfParallelism = 1,
            ColumnNames = new[] { "Value" }
        };
        await _bulkCopyInt.InitAsync();*/

        /*await _connection.ExecuteStatementAsync("DROP TABLE IF EXISTS benchmark_bulk_insert_entity");
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
    ENGINE Null");*/

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
        await using var reader = new ClickHouseBulkReader(IntRows, new[] {"Value"}, "benchmark_bulk_insert_int64");
        await _connection.PostStreamAsync(null, reader.GetStream(true), true, CancellationToken.None);
    }

    [Benchmark]
    public async Task BulkInsertEntity()
    {
        await _bulkCopyEntity.WriteToServerAsync(ObjectPrimitiveTableTypeRows);
    }

    [Benchmark]
    public async Task NewBulkInsertEntity()
    {
        await using var reader = new ClickHouseBulkReader(PrimitiveTableTypeRows, _sortedColumns, "benchmark_bulk_insert_entity");
        await _connection.PostStreamAsync(null, reader.GetStream(true), true, CancellationToken.None);
    }

    [Benchmark]
    public async Task NewAsyncBulkInsertEntity()
    {
        await using var reader = new ClickHouseBulkAsyncReader<PrimitiveTableType>(GetAsyncPrimitiveTableTypeRows(), _sortedColumns, "benchmark_bulk_insert_entity");
        await _connection.PostStreamAsync(null, reader.GetStream(true), true, CancellationToken.None);
    }
}

using System.Numerics;
using System.Reflection;
using ClickHouse.Client.ADO;
using ClickHouse.Client.BulkExtension.Annotation;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.BulkExtension.Tests;

public class Tests
{
    private ClickHouseBulkReader _reader;
    private ClickHouseBulkAsyncReader<ComplexTableType> _asyncReader;
    private ClickHouseConnection _connection;

    private readonly string[] _primitiveTypeColumns = typeof(PrimitiveTableType)
        .GetProperties()
        .Select(x => x.GetCustomAttribute<ClickHouseColumnAttribute>()?.Name ?? x.Name)
        .ToArray();

    private readonly string[] _complexTypeColumns = typeof(ComplexTableType)
        .GetProperties()
        .Select(x => x.GetCustomAttribute<ClickHouseColumnAttribute>()?.Name ?? x.Name)
        .ToArray();

    private ClickHouseBulkCopy _bulkCopyEntity;

    private const int Count = 10;

    private IEnumerable<ComplexTableType> Data
    {
        get
        {
            for (int i = 0; i < Count; i++)
            {
                yield return new ComplexTableType()
                {
                    GuidColumn = Guid.NewGuid(),
                    BooleanColumn = i % 2 == 0,
                    StringColumn = $"String{i}",
                    DecimalColumn = i + 0.1m,
                    DoubleColumn = i + 0.2,
                    FloatColumn = i + 0.3f,
                    IntColumn = i + 3,
                    LongColumn = i,
                    ShortColumn = (short)i,
                    BigInteger128Column = BigInteger.Pow(2 * (i % 2 == 0 ? 1 : -1), 127) + (i % 2 == 0 ? -1 : 1),
                    BigInteger256Column = BigInteger.Pow(2 * (i % 2 == 0 ? 1 : -1), 255) + (i % 2 == 0 ? -1 : 1),
                    BigIntegerU128Column = BigInteger.Pow(2, 128) - 1,
                    BigIntegerU256Column = BigInteger.Pow(2, 256) - 1,
                    DateTimeColumn = DateTime.Now.AddMinutes(i),
                    ArrayColumn = Enumerable.Range(0, i).Select(y => y).ToList(),
                    MapColumn = Enumerable.Range(0, i).ToDictionary(y => $"Key{y}", y => (long)y),
                    TupleColumn = new Tuple<string, int, long>($"String{i}", i, i),
                    ValueTupleColumn = ($"String{i}", i, i)
                };
            }
        }
    }

    private async IAsyncEnumerable<ComplexTableType> GetAsyncData()
    {
        for (int i = 0; i < Count; i++)
        {
            yield return new ComplexTableType()
            {
                GuidColumn = Guid.NewGuid(),
                BooleanColumn = i % 2 == 0,
                StringColumn = $"String{i}",
                DecimalColumn = i + 0.1m,
                DoubleColumn = i + 0.2,
                FloatColumn = i + 0.3f,
                IntColumn = i + 3,
                LongColumn = i,
                ShortColumn = (short)i,
                BigInteger128Column = BigInteger.Pow(2 * (i % 2 == 0 ? 1 : -1), 127) + (i % 2 == 0 ? -1 : 1),
                BigInteger256Column = BigInteger.Pow(2 * (i % 2 == 0 ? 1 : -1), 255) + (i % 2 == 0 ? -1 : 1),
                BigIntegerU128Column = BigInteger.Pow(2, 128) - 1,
                BigIntegerU256Column = BigInteger.Pow(2, 256) - 1,
                DateTimeColumn = DateTime.Now.AddMinutes(i),
                ArrayColumn = Enumerable.Range(0, i).Select(y => y).ToList(),
                MapColumn = Enumerable.Range(0, i).ToDictionary(y => $"Key{y}", y => (long)y),
                TupleColumn = new Tuple<string, int, long>($"String{i}", i, i),
                ValueTupleColumn = ($"String{i}", i, i)
            };
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
                    ($"String{i}", i, i)
                };
            }
        }
    }

    [OneTimeSetUp]
    public async Task Setup()
    {
        _reader = new ClickHouseBulkReader(Data, _complexTypeColumns, "test_bulk_insert");
        _asyncReader = new ClickHouseBulkAsyncReader<ComplexTableType>(GetAsyncData(), _complexTypeColumns, "test_bulk_insert");

        var connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION");
        _connection = new ClickHouseConnection(connectionString);

        await _connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test_bulk_insert");
        await _connection.ExecuteStatementAsync(@"
CREATE TABLE IF NOT EXISTS test_bulk_insert 
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
    MapColumn Map(String, Int64),
    TupleColumn Tuple(s String, i32 Int32, i64 Int64),
    ValueTupleColumn Tuple(s String, i32 Int32, i64 Int64)
) 
    ENGINE Memory");

        _bulkCopyEntity = new ClickHouseBulkCopy(_connection)
        {
            DestinationTableName = "test_bulk_insert",
            BatchSize = Count,
            MaxDegreeOfParallelism = 1,
            ColumnNames = _primitiveTypeColumns
        };
        await _bulkCopyEntity.InitAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        await _connection.DisposeAsync();
    }

    [Test]
    public async Task Test1()
    {
        var stream = _reader.GetStream(true);
        await _connection.PostStreamAsync(null, stream, true, CancellationToken.None);
        await _reader.CompleteAsync();
    }

    [Test]
    public async Task Test2()
    {
        var stream = _asyncReader.GetStream(true);
        await _connection.PostStreamAsync(null, stream, true, CancellationToken.None);
        await _asyncReader.CompleteAsync();
    }

    [Test]
    public async Task Test3()
    {
        await _bulkCopyEntity.WriteToServerAsync(ObjectPrimitiveTableTypeRows);
    }
}
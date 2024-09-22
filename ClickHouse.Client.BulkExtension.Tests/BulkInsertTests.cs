using System.Numerics;
using System.Reflection;
using ClickHouse.Client.ADO;
using ClickHouse.Client.BulkExtension.Annotation;
using ClickHouse.Client.Utility;

namespace ClickHouse.Client.BulkExtension.Tests;

public class BulkInsertTests
{
    private ClickHouseBulkReader _reader;
    private ClickHouseBulkAsyncReader<ComplexTableType> _asyncReader;
    private ClickHouseConnection _connection;

    private readonly string[] _complexTypeColumns = typeof(ComplexTableType)
        .GetProperties()
        .Select(x => x.GetCustomAttribute<ClickHouseColumnAttribute>()?.Name ?? x.Name)
        .ToArray();

    private const int Count = 100_000;

    private IEnumerable<ComplexTableType> Data
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
    private async IAsyncEnumerable<ComplexTableType> GetAsyncData()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
    {
        for (int i = 0; i < Count; i++)
        {
            yield return GetEntity(i);
        }
    }

    private const string StringForNoAlloc = "String";

    private static ComplexTableType GetEntity(int i)
    {
        return new ComplexTableType()
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
            BigInteger128Column = BigInteger.Pow(2 * (i % 2 == 0 ? 1 : -1), 127) + (i % 2 == 0 ? -1 : 1),
            BigInteger256Column = BigInteger.Pow(2 * (i % 2 == 0 ? 1 : -1), 255) + (i % 2 == 0 ? -1 : 1),
            BigIntegerU128Column = BigInteger.Pow(2, 128) - 1,
            BigIntegerU256Column = BigInteger.Pow(2, 256) - 1,
            DateTimeColumn = DateTime.Now.AddMinutes(i),
            ArrayColumn = Enumerable.Range(0, (i % 3) + 1).Select(y => y).ToList(),
            MapColumn = Enumerable.Range(0, (i % 3) + 1).ToDictionary(y => y, y => StringForNoAlloc),
            TupleColumn = new Tuple<string, int, long>(StringForNoAlloc, i, i),
            ValueTupleColumn = (StringForNoAlloc, i, i)
        };
    }

    [OneTimeSetUp]
    public async Task Setup()
    {
        _reader = new ClickHouseBulkReader(Data, _complexTypeColumns, "test_bulk_insert", true);
        _asyncReader = new ClickHouseBulkAsyncReader<ComplexTableType>(GetAsyncData(), _complexTypeColumns, "test_bulk_insert", true);

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
    DateTimeColumn datetime64(6),
    ArrayColumn Array(Int32),
    MapColumn Map(Int32, String),
    TupleColumn Tuple(s String, i32 Int32, i64 Int64),
    ValueTupleColumn Tuple(s String, i32 Int32, i64 Int64)
) 
    ENGINE Memory");
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        await _connection.DisposeAsync();
    }

    [Test]
    public async Task Test1()
    {
        await _connection.PostStreamAsync(null, _reader.GetStreamWriteCallBack(), true, CancellationToken.None);
    }

    [Test]
    public async Task Test2()
    {
        await _connection.PostStreamAsync(null, _asyncReader.GetStreamWriteCallBack(), true, CancellationToken.None);
    }
}
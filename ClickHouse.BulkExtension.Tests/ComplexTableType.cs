using System.Numerics;
using ClickHouse.BulkExtension.Annotation;

namespace ClickHouse.BulkExtension.Tests;

public class ComplexTableType
{
    [ClickHouseColumn(Name = "Qwerty")]
    public Guid GuidColumn { get; set; }
    public bool BooleanColumn { get; set; }
    public string StringColumn { get; set; } = null!;
    public byte[] BytesColumn { get; set; } = null!;
    public Memory<byte> MemoryColumn { get; set; }

    [ClickHouseColumn(Precision = 16, Scale = 6)]
    public decimal DecimalColumn { get; set; }
    public double DoubleColumn { get; set; }
    public float FloatColumn { get; set; }
    public int IntColumn { get; set; }
    public long LongColumn { get; set; }
    public short ShortColumn { get; set; }
    public BigInteger BigInteger128Column { get; set; }

    [ClickHouseColumn(BigIntegerBits = BigIntegerBits.Bits256)]
    public BigInteger BigInteger256Column { get; set; }
    public BigInteger BigIntegerU128Column { get; set; }

    [ClickHouseColumn(BigIntegerBits = BigIntegerBits.Bits256)]
    public BigInteger BigIntegerU256Column { get; set; }

    [ClickHouseColumn(DateTimePrecision = DateTimePrecision.Microsecond)]
    public DateTime DateTimeColumn { get; set; }
    public IEnumerable<int> ArrayInt32Column { get; set; } = null!;
    public IEnumerable<string> ArrayStringColumn { get; set; } = null!;
    public Dictionary<int, string> MapColumn { get; set; } = null!;
    public Tuple<string, int, long> TupleColumn { get; set; } = null!;
    public (string S, int X, long Y) ValueTupleColumn { get; set; }
}

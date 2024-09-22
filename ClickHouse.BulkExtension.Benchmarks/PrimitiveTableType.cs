using ClickHouse.BulkExtension.Annotation;

namespace ClickHouse.BulkExtension.Benchmarks;

public struct PrimitiveTableType
{
    [ClickHouseColumn(Name = "Qwerty")]
    public Guid GuidColumn { get; set; }
    public bool BooleanColumn { get; set; }
    public string StringColumn { get; set; }

    [ClickHouseColumn(Precision = 18, Scale = 6)]
    public decimal DecimalColumn { get; set; }
    public double DoubleColumn { get; set; }
    public float FloatColumn { get; set; }
    public int IntColumn { get; set; }
    public long LongColumn { get; set; }
    public short ShortColumn { get; set; }

    [ClickHouseColumn(DateTimePrecision = DateTimePrecision.Microsecond)]
    public DateTime DateTimeColumn { get; set; }
    public (string S, int X, long Y) ValueTupleColumn { get; set; }
}

namespace ClickHouse.BulkExtension.Annotation;

[AttributeUsage(AttributeTargets.Property)]
public class ClickHouseColumnAttribute : Attribute
{
    public string Name { get; set; }
    public byte Precision { get; set; }
    public byte Scale { get; set; }
    public DateTimePrecision DateTimePrecision { get; set; } = DateTimePrecision.Millisecond;
    public BigIntegerBits BigIntegerBits { get; set; } = BigIntegerBits.Bits128;
}

public enum BigIntegerBits : byte
{
    Bits128,
    Bits256
}

public enum DateTimePrecision : byte
{
    Second,
    Millisecond = 3,
    Microsecond = 6,
    Nanosecond = 9
}
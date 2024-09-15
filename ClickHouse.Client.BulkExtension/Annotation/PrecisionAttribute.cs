namespace ClickHouse.Client.BulkExtension.Annotation;

[AttributeUsage(AttributeTargets.Property)]
public class PrecisionAttribute : Attribute
{
    public int Value { get; }

    public PrecisionAttribute(int value)
    {
        Value = value;
    }
}
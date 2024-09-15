namespace ClickHouse.Client.BulkExtension.Annotation;

[AttributeUsage(AttributeTargets.Property)]
public class ScaleAttribute : Attribute
{
    public int Value { get; }

    public ScaleAttribute(int value)
    {
        Value = value;
    }
}
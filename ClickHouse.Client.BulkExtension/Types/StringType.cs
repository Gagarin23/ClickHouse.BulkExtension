using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class StringType
{
    public static readonly MethodInfo WriteMethod = typeof(StringType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly StringType Instance = new StringType();

    private StringType() { }

    public void Write(BinaryWriter writer, string value)
    {
        writer.Write(value);
    }
}
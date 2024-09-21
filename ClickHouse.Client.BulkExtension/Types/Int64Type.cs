using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class Int64Type
{
    public static readonly MethodInfo WriteMethod = typeof(Int64Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly Int64Type Instance = new Int64Type();

    private Int64Type() { }

    public void Write(BinaryWriter writer, long value)
    {
        writer.Write(value);
    }
}
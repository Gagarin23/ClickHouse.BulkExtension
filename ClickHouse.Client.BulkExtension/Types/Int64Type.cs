using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class Int64Type : IntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(Int64Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly Int64Type Instance = new Int64Type();

    private Int64Type() { }

    public void Write(BinaryWriter writer, long value)
    {
        writer.Write(value);
    }
}
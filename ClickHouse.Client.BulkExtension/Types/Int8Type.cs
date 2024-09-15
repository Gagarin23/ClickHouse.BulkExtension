using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class Int8Type : IntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(Int8Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly Int8Type Instance = new Int8Type();

    private Int8Type() { }

    public void Write(BinaryWriter writer, sbyte value)
    {
        writer.Write(value);
    }
}
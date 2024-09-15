using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class Int16Type : IntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(Int16Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly Int16Type Instance = new Int16Type();

    private Int16Type() { }

    public void Write(BinaryWriter writer, short value)
    {
        writer.Write(value);
    }
}
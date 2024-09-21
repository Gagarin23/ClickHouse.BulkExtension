using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class Int32Type
{
    public static readonly MethodInfo WriteMethod = typeof(Int32Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly Int32Type Instance = new Int32Type();

    private Int32Type() { }

    public void Write(BinaryWriter writer, int value)
    {
        writer.Write(value);
    }
}
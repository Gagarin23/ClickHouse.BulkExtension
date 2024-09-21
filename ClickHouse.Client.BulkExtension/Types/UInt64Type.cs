using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class UInt64Type
{
    public static readonly MethodInfo WriteMethod = typeof(UInt64Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly UInt64Type Instance = new UInt64Type();

    private UInt64Type() { }

    public void Write(BinaryWriter writer, ulong value)
    {
        writer.Write(value);
    }
}
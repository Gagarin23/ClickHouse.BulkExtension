using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class UInt16Type
{
    public static readonly MethodInfo WriteMethod = typeof(UInt16Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly UInt16Type Instance = new UInt16Type();

    private UInt16Type() { }

    public void Write(BinaryWriter writer, ushort value)
    {
        writer.Write(value);
    }
}
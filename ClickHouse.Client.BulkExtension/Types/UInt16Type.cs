using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class UInt16Type : IntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(UInt16Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly UInt16Type Instance = new UInt16Type();

    private UInt16Type() { }

    protected override bool Signed => false;
    public void Write(BinaryWriter writer, ushort value)
    {
        writer.Write(value);
    }
}
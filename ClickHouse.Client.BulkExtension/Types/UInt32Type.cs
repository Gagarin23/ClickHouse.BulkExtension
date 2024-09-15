using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class UInt32Type : IntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(UInt32Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly UInt32Type Instance = new UInt32Type();

    private UInt32Type() { }

    protected override bool Signed => false;
    public void Write(BinaryWriter writer, uint value)
    {
        writer.Write(value);
    }
}
using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class UInt8Type : IntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(UInt8Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly UInt8Type Instance = new UInt8Type();

    private UInt8Type() { }

    protected override bool Signed => false;
    public void Write(BinaryWriter writer, byte value)
    {
        writer.Write(value);
    }
}
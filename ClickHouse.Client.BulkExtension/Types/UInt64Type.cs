using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class UInt64Type : IntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(UInt64Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly UInt64Type Instance = new UInt64Type();

    private UInt64Type() { }

    protected override bool Signed => false;
    public void Write(BinaryWriter writer, ulong value)
    {
        writer.Write(value);
    }
}
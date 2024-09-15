using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class UInt256Type : AbstractBigIntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(AbstractBigIntegerType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly UInt256Type Instance = new UInt256Type();

    private UInt256Type() { }

    public override int Size => 32;

    protected override bool Signed => false;
}
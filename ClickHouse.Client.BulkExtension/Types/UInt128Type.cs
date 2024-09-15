using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class UInt128Type : AbstractBigIntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(AbstractBigIntegerType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly UInt128Type Instance = new UInt128Type();

    private UInt128Type() { }

    public override int Size => 16;

    protected override bool Signed => false;
}
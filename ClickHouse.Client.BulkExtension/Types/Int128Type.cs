using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class Int128Type : AbstractBigIntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(AbstractBigIntegerType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly Int128Type Instance = new Int128Type();

    private Int128Type() { }

    public override int Size => 16;
}
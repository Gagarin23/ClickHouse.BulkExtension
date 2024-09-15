using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class Int256Type : AbstractBigIntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(AbstractBigIntegerType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly Int256Type Instance = new Int256Type();

    private Int256Type() { }

    public override int Size => 32;
}
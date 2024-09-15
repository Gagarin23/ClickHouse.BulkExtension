using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;

namespace ClickHouse.Client.BulkExtension.Types;

class PointType : TupleType<double>
{
    public static readonly MethodInfo WriteMethod = typeof(PointType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly PointType Instance = new PointType();

    private PointType() { }

    public override void Write(BinaryWriter writer, double value)
    {
        Float64Type.Instance.Write(writer, value);
    }
}
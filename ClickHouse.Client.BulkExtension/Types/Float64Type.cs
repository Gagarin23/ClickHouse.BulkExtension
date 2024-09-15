using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class Float64Type
{
    public static readonly MethodInfo WriteMethod = typeof(Float64Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly Float64Type Instance = new Float64Type();

    private Float64Type() { }

    public void Write(BinaryWriter writer, double value)
    {
        writer.Write(value);
    }
}
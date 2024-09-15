using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class Float32Type
{
    public static readonly MethodInfo WriteMethod = typeof(Float32Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly Float32Type Instance = new Float32Type();

    private Float32Type() { }

    public void Write(BinaryWriter writer, float value)
    {
        writer.Write(value);
    }
}
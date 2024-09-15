using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class BooleanType
{
    public static readonly MethodInfo WriteMethod = typeof(BooleanType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly BooleanType Instance = new BooleanType();

    private BooleanType() { }

    public void Write(BinaryWriter writer, bool value)
    {
        writer.Write(value);
    }
}
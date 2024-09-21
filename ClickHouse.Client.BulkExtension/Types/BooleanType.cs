using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class BooleanType
{
    public static readonly MethodInfo WriteMethod = typeof(BooleanType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly BooleanType Instance = new BooleanType();

    private BooleanType() { }

    public void Write(BinaryWriter writer, bool value)
    {
        // yes, this is a bit of overkill for a single byte, but it's the most efficient way to write a single byte,
        // because BinaryWriter.Write(bool) call Stream.WriteByte(bool) which allocated new byte[1]
        Span<byte> buffer = stackalloc byte[1];
        buffer[0] = value ? (byte)1 : (byte)0;
        writer.Write(buffer);
    }
}
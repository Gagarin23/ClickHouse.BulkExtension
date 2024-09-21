using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class Int8Type
{
    public static readonly MethodInfo WriteMethod = typeof(Int8Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly Int8Type Instance = new Int8Type();

    private Int8Type() { }

    public void Write(BinaryWriter writer, sbyte value)
    {
        // yes, this is a bit of overkill for a single byte, but it's the most efficient way to write a single byte,
        // because BinaryWriter.Write(bool) call Stream.WriteByte(bool) which allocated new byte[1]
        Span<byte> buffer = stackalloc byte[1] { (byte)value };
        writer.Write(buffer);
    }
}
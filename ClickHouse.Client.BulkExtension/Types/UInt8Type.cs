using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class UInt8Type
{
    public static readonly MethodInfo WriteMethod = typeof(UInt8Type).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly UInt8Type Instance = new UInt8Type();

    private UInt8Type() { }

    public void Write(BinaryWriter writer, byte value)
    {
        // yes, this is a bit of overkill for a single byte, but it's the most efficient way to write a single byte,
        // because BinaryWriter.Write(bool) call Stream.WriteByte(bool) which allocated new byte[1]
        Span<byte> buffer = stackalloc byte[1] { value };
        writer.Write(buffer);
    }
}
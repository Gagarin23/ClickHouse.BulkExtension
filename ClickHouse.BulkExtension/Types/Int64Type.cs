using System.Buffers.Binary;

namespace ClickHouse.BulkExtension.Types;

class Int64Type
{
    public static readonly Int64Type Instance = new Int64Type();

    private Int64Type() { }

    public int Write(Memory<byte> buffer, long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(buffer.Span[..sizeof(long)], value);
        return sizeof(long);
    }
}
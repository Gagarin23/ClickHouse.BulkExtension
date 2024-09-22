using System.Buffers.Binary;

namespace ClickHouse.BulkExtension.Types;

class Int16Type
{
    public static readonly Int16Type Instance = new Int16Type();

    private Int16Type() { }

    public int Write(Memory<byte> buffer, short value)
    {
        BinaryPrimitives.WriteInt16LittleEndian(buffer.Span[..sizeof(short)], value);
        return sizeof(short);
    }
}
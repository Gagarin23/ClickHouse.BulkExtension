using System.Buffers.Binary;

namespace ClickHouse.Client.BulkExtension.Types;

class UInt32Type
{
    public static readonly UInt32Type Instance = new UInt32Type();

    private UInt32Type() { }

    public int Write(Memory<byte> buffer, uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.Span[..sizeof(uint)], value);
        return sizeof(uint);
    }
}
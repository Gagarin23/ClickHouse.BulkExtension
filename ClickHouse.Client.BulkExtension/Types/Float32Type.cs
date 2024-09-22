using System.Buffers.Binary;

namespace ClickHouse.Client.BulkExtension.Types;

class Float32Type
{
    public static readonly Float32Type Instance = new Float32Type();

    private Float32Type() { }

    public int Write(Memory<byte> buffer, float value)
    {
        BinaryPrimitives.WriteSingleLittleEndian(buffer.Span[..sizeof(float)], value);
        return sizeof(float);
    }
}
using System.Buffers.Binary;

namespace ClickHouse.BulkExtension.Types;

class Float64Type
{
    public static readonly Float64Type Instance = new Float64Type();

    private Float64Type() { }

    public int Write(Memory<byte> buffer, double value)
    {
        BinaryPrimitives.WriteDoubleLittleEndian(buffer.Span[..sizeof(double)], value);
        return sizeof(double);
    }
}
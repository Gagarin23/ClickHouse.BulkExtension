using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace ClickHouse.BulkExtension.Types;

class Float32Type
{
    public static readonly Float32Type Instance = new Float32Type();

    private Float32Type() { }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Write(Memory<byte> buffer, float value)
    {
        BinaryPrimitives.WriteSingleLittleEndian(buffer.Span[..sizeof(float)], value);
        return sizeof(float);
    }
}
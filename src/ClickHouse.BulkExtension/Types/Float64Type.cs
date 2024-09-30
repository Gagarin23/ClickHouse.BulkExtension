using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace ClickHouse.BulkExtension.Types;

class Float64Type
{
    public static readonly Float64Type Instance = new Float64Type();

    private Float64Type() { }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Write(Memory<byte> buffer, double value)
    {
        BinaryPrimitives.WriteDoubleLittleEndian(buffer.Span[..sizeof(double)], value);
        return sizeof(double);
    }
}
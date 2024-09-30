using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace ClickHouse.BulkExtension.Types;

class Int64Type
{
    public static readonly Int64Type Instance = new Int64Type();

    private Int64Type() { }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Write(Memory<byte> buffer, long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(buffer.Span[..sizeof(long)], value);
        return sizeof(long);
    }
}
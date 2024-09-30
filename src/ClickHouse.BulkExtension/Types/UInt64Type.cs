using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace ClickHouse.BulkExtension.Types;

class UInt64Type
{
    public static readonly UInt64Type Instance = new UInt64Type();

    private UInt64Type() { }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Write(Memory<byte> buffer, ulong value)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.Span[..sizeof(ulong)], value);
        return sizeof(ulong);
    }
}
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace ClickHouse.BulkExtension.Types;

class UInt16Type
{
    public static readonly UInt16Type Instance = new UInt16Type();

    private UInt16Type() { }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Write(Memory<byte> buffer, ushort value)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(buffer.Span[..sizeof(ushort)], value);
        return sizeof(ushort);
    }
}
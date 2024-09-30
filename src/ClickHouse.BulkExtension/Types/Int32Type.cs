using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace ClickHouse.BulkExtension.Types;

class Int32Type
{
    public static readonly Int32Type Instance = new Int32Type();

    private Int32Type() { }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Write(Memory<byte> buffer, int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buffer.Span[..sizeof(int)], value);
        return sizeof(int);
    }
}
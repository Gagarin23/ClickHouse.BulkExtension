using System.Buffers;
using System.Text;

namespace ClickHouse.BulkExtension.Types;

class StringType
{
    public static readonly StringType Instance = new StringType();

    private StringType() { }

    public int Write(Memory<byte> buffer, string value)
    {
        var span = buffer.Span;
        if (value.Length <= 127 / 3)
        {
            var written = Encoding.UTF8.GetBytes(value, span[1..]);
            span[0] = (byte)written; // bypass call to Write7BitEncodedInt
            return written + 1;
        }

        var rented = ArrayPool<byte>.Shared.Rent(value.Length * 3); // max expansion: each char -> 3 bytes
        try
        {
            var actualByteCount = Encoding.UTF8.GetBytes(value, rented);
            var written = buffer.Write7BitEncodedInt(actualByteCount);
            rented.AsSpan(0, actualByteCount).CopyTo(span[written..]);
            return actualByteCount + written;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
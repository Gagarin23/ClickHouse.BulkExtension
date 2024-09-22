using System.Numerics;

namespace ClickHouse.Client.BulkExtension.Types;

class BigIntegerType
{
    public static readonly BigIntegerType Instance128 = new BigIntegerType(16);
    public static readonly BigIntegerType Instance256 = new BigIntegerType(32);

    private readonly byte _size;

    private BigIntegerType(byte size)
    {
        _size = size;
    }

    public int Write(Memory<byte> buffer, BigInteger value)
    {
        Span<byte> bigIntBytes = buffer.Span[.._size];
        bigIntBytes.Clear();
        var bitsLength = value.GetBitLength();
        var isNegative = value.Sign < 0;

        if (isNegative)
        {
            bigIntBytes.Fill(0xFF);
            var bi = new BigInteger(bigIntBytes);
            value -= bi + 1;
        }

        var written = 0;
        if (!isNegative && (_size == 16 && bitsLength == 128 || _size == 32 && bitsLength == 256))
        {
            value.TryWriteBytes(bigIntBytes, out written, isUnsigned: true);
        }
        else
        {
            value.TryWriteBytes(bigIntBytes, out written);
        }

        return _size;
    }
}
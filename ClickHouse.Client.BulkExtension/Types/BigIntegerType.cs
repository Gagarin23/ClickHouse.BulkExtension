using System.Numerics;
using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class BigIntegerType
{
    public static readonly MethodInfo WriteMethod = typeof(BigIntegerType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly BigIntegerType Instance128 = new BigIntegerType(16);
    public static readonly BigIntegerType Instance256 = new BigIntegerType(32);

    private readonly byte _size;

    private BigIntegerType(byte size)
    {
        _size = size;
    }

    public void Write(BinaryWriter writer, BigInteger value)
    {
        Span<byte> bigIntBytes = stackalloc byte[_size];
        var bitsLength = value.GetBitLength();
        var isNegative = value.Sign < 0;

        if (isNegative)
        {
            bigIntBytes.Fill(0xFF);
            var bi = new BigInteger(bigIntBytes);
            value -= bi + 1;
        }

        if (!isNegative && (_size == 16 && bitsLength == 128 || _size == 32 && bitsLength == 256))
        {
            value.TryWriteBytes(bigIntBytes, out _, isUnsigned: true);
        }
        else
        {
            value.TryWriteBytes(bigIntBytes, out _);
        }

        writer.Write(bigIntBytes);
    }
}
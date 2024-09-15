using System.Numerics;

namespace ClickHouse.Client.BulkExtension.Types.Base;

abstract class AbstractBigIntegerType : IntegerType
{
    public virtual int Size { get; }

    public virtual void Write(BinaryWriter writer, BigInteger value)
    {
        if (value < 0 && !Signed)
            throw new ArgumentException("Cannot convert negative BigInteger to UInt");

        var bigIntBytes = value.ToByteArray();
        var decimalBytes = new byte[Size];

        var lengthToCopy = bigIntBytes.Length;
        if (!Signed && bigIntBytes[bigIntBytes.Length - 1] == 0)
            lengthToCopy = bigIntBytes.Length - 1;

        if (lengthToCopy > Size)
            throw new OverflowException($"Got {lengthToCopy} bytes, {Size} expected");

        Array.Copy(bigIntBytes, decimalBytes, lengthToCopy);

        // If a negative BigInteger is not long enough to fill the whole buffer,
        // the remainder needs to be filled with 0xFF
        if (value < 0)
        {
            for (var i = bigIntBytes.Length; i < Size; i++)
            {
                decimalBytes[i] = 0xFF;
            }
        }
        writer.Write(decimalBytes);
    }
}
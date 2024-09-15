using System.Numerics;
using System.Reflection;
using ClickHouse.Client.BulkExtension.Numerics;

namespace ClickHouse.Client.BulkExtension.Types;

class DecimalType
{
    public static readonly MethodInfo DecimalWriteMethod = typeof(DecimalType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance, new []{typeof(BinaryWriter), typeof(decimal)})!;
    public static readonly MethodInfo ClickHouseDecimalWriteMethod = typeof(DecimalType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance, new []{typeof(BinaryWriter), typeof(ClickHouseDecimal)})!;

    private readonly int _precision;
    private readonly int _scale;

    public DecimalType(int precision, int scale)
    {
        _precision = precision;
        _scale = scale;
    }

    /// <summary>
    ///     Gets size of type in bytes
    /// </summary>
    private int Size => GetSizeFromPrecision(_precision);

    public void Write(BinaryWriter writer, decimal value)
    {
        var @decimal = new ClickHouseDecimal(value);
        Write(writer, @decimal);
    }

    public void Write(BinaryWriter writer, ClickHouseDecimal value)
    {
        try
        {
            var mantissa = ClickHouseDecimal.ScaleMantissa(value, _scale);
            WriteBigInteger(writer, mantissa);
        }
        catch (OverflowException)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, "Value cannot be represented");
        }
    }

    private static int GetSizeFromPrecision(int precision)
    {
        return precision switch
        {
            >= 1 and <= 9   => 4,
            >= 10 and <= 18 => 8,
            >= 19 and <= 38 => 16,
            >= 39 and <= 76 => 32,
            _               => throw new ArgumentOutOfRangeException(nameof(precision))
        };
    }

    private void WriteBigInteger(BinaryWriter writer, BigInteger value)
    {
        var bigIntBytes = value.ToByteArray();
        var decimalBytes = new byte[Size];

        if (bigIntBytes.Length > Size)
            throw new OverflowException($"Trying to write {bigIntBytes.Length} bytes, at most {Size} expected");

        bigIntBytes.CopyTo(decimalBytes, 0);

        // If a negative BigInteger is not long enough to fill the whole buffer,
        // the remainder needs to be filled with 0xFF
        if (value.Sign < 0)
        {
            for (var i = bigIntBytes.Length; i < Size; i++)
            {
                decimalBytes[i] = 0xFF;
            }
        }
        writer.Write(decimalBytes);
    }
}
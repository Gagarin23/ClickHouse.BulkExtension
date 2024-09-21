using System.Numerics;

namespace ClickHouse.Client.BulkExtension.Numerics;

/// <summary>
///     Arbitrary precision decimal.
///     All operations are exact, except for division. Division never determines more digits than the given precision.
///     Based on: https://gist.github.com/JcBernack/0b4eef59ca97ee931a2f45542b9ff06d
///     Based on https://stackoverflow.com/a/4524254
///     Original Author: Jan Christoph Bernack (contact: jc.bernack at gmail.com)
/// </summary>
public readonly struct ClickHouseDecimal
{
    /// <summary>
    ///     Sets the global maximum precision of division operations.
    /// </summary>
    public static readonly int MaxDivisionPrecision = 50;

    public ClickHouseDecimal(decimal value)
        : this()
    {
        // Slightly wasteful, but seems to be the cheapest way to get scale
        Span<int> parts = stackalloc int[4];
        decimal.GetBits(value, parts);
        var scale = parts[3] >> 16 & 0x7F;
        var isNegative = (parts[3] & 0x80000000) != 0;

        Span<byte> data = stackalloc byte[3 * sizeof(int) + 1];
        WriteInt(parts[0], data, 0);
        WriteInt(parts[1], data, sizeof(int));
        WriteInt(parts[2], data, 2 * sizeof(int));

        var mantissa = new BigInteger(data);
        if (isNegative)
            mantissa = BigInteger.Negate(mantissa);

        Mantissa = mantissa;
        Scale = scale;
    }

    public ClickHouseDecimal(BigInteger mantissa, int scale)
        : this()
    {
        if (scale < 0)
            throw new ArgumentException("Scale cannot be <0", nameof(scale));
        // Normalize(ref mantissa, ref scale);

        Mantissa = mantissa;
        Scale = scale;
    }

    public readonly BigInteger Mantissa { get; }

    public readonly int Scale { get; }

    public static ClickHouseDecimal Zero => new ClickHouseDecimal(0, 0);

    public static ClickHouseDecimal One => new ClickHouseDecimal(1, 0);

    public int Sign => Mantissa.Sign;

    /// <summary>
    ///     Removes trailing zeros on the mantissa
    /// </summary>
    private static void Normalize(ref BigInteger mantissa, ref int scale)
    {
        if (mantissa.IsZero)
        {
            scale = 0;
        }
        else
        {
            BigInteger remainder = 0;
            while (remainder == 0 && scale > 0)
            {
                var shortened = BigInteger.DivRem(mantissa, 10, out remainder);
                if (remainder == 0)
                {
                    mantissa = shortened;
                    scale--;
                }
            }
        }
    }

    /// <summary>
    ///     Truncate the number to the given precision by removing the least significant digits.
    /// </summary>
    private static void Truncate(ref BigInteger mantissa, ref int scale,
        int precision)
    {
        // remove the least significant digits, as long as the number of digits is higher than the given Precision
        var digits = NumberOfDigits(mantissa);
        var digitsToRemove = Math.Max(digits - precision, 0);
        digitsToRemove = Math.Min(digitsToRemove, scale);
        mantissa /= BigInteger.Pow(10, digitsToRemove);
        scale -= digitsToRemove;
    }

    public ClickHouseDecimal Truncate(int precision = 0)
    {
        var mantissa = Mantissa;
        var scale = Scale;
        Truncate(ref mantissa, ref scale, precision);
        return new ClickHouseDecimal(mantissa, scale);
    }

    public ClickHouseDecimal Floor()
    {
        return Truncate(NumberOfDigits(Mantissa) - Scale);
    }

    public static int NumberOfDigits(BigInteger value)
    {
        return value == 0 ? 0 : (int)Math.Ceiling(BigInteger.Log10(value * value.Sign));
    }

    public static implicit operator ClickHouseDecimal(int value)
    {
        return new ClickHouseDecimal(value, 0);
    }

    public static implicit operator ClickHouseDecimal(double value)
    {
        var mantissa = (BigInteger)value;
        var scale = 0;
        double scaleFactor = 1;
        while (Math.Abs(value * scaleFactor - (double)mantissa) > 0)
        {
            scale += 1;
            scaleFactor *= 10;
            mantissa = (BigInteger)(value * scaleFactor);
        }
        return new ClickHouseDecimal(mantissa, scale);
    }

    public static implicit operator ClickHouseDecimal(decimal value)
    {
        return new ClickHouseDecimal(value);
    }

    public static explicit operator double(ClickHouseDecimal value)
    {
        return (double)value.Mantissa / Math.Pow(10, value.Scale);
    }

    public static explicit operator float(ClickHouseDecimal value)
    {
        return Convert.ToSingle((double)value);
    }

    public static explicit operator decimal(ClickHouseDecimal value)
    {
        var mantissa = value.Mantissa;
        var scale = value.Scale;

        var negative = mantissa < 0;
        if (negative)
        {
            mantissa = BigInteger.Negate(mantissa);
        }

        var bytesCount = mantissa.GetByteCount();
        Span<byte> numberBytes = stackalloc byte[bytesCount];
        mantissa.TryWriteBytes(numberBytes, out _);
        switch (numberBytes.Length)
        {
            case 13 when numberBytes[12] == 0:
                break;
            case (> 12):
                ThrowDecimalOverflowException();
                break;
        }

        Span<byte> data = stackalloc byte[3 * sizeof(int)];
        numberBytes.CopyTo(data);

        var part0 = BitConverter.ToInt32(data);
        var part1 = BitConverter.ToInt32(data[4..]);
        var part2 = BitConverter.ToInt32(data[8..]);

        var result = new decimal(part0, part1, part2, negative, (byte)scale);
        return result;
    }

    public bool Equals(ClickHouseDecimal other)
    {
        var maxScale = Math.Max(Scale, other.Scale);

        return ScaleMantissa(this, maxScale) == ScaleMantissa(other, maxScale);
    }

    public override bool Equals(object obj)
    {
        return CompareTo(obj) == 0;
    }

    public override int GetHashCode()
    {
        unchecked
        {
            return Mantissa.GetHashCode() * 397 ^ Scale;
        }
    }

    public int CompareTo(object obj)
    {
        return obj is ClickHouseDecimal cbi ? CompareTo(cbi) : 1;
    }

    public int CompareTo(ClickHouseDecimal other)
    {
        var maxScale = Math.Max(Scale, other.Scale);
        var left_mantissa = ScaleMantissa(this, maxScale);
        var right_mantissa = ScaleMantissa(other, maxScale);

        return left_mantissa.CompareTo(right_mantissa);
    }

    public int CompareTo(decimal other)
    {
        return CompareTo((ClickHouseDecimal)other);
    }

    internal static BigInteger ScaleMantissa(ClickHouseDecimal value, int scale)
    {
        if (scale == value.Scale)
            return value.Mantissa;
        if (scale < value.Scale)
            return value.Mantissa / BigInteger.Pow(10, value.Scale - scale);
        return value.Mantissa * BigInteger.Pow(10, scale - value.Scale);
    }

    private static void WriteInt(int value, Span<byte> array, int index)
    {
        array[index + 0] = (byte)value;
        array[index + 1] = (byte)(value >> 8);
        array[index + 2] = (byte)(value >> 0x10);
        array[index + 3] = (byte)(value >> 0x18);
    }

    // [DoesNotReturn]
    private static void ThrowDecimalOverflowException()
    {
        throw new OverflowException("Value cannot be represented as System.Decimal");
    }
}
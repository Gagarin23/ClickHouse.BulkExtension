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
        // Масштабируем значение
        decimal scaledValue = ScaleDecimal(value, _scale);

        switch (_precision)
        {
            // В зависимости от precision выбираем тип данных
            // Decimal32
            case <= 9:
                int intValue = decimal.ToInt32(scaledValue);
                writer.Write(intValue);
                break;
            // Decimal64
            case <= 18:
                long longValue = decimal.ToInt64(scaledValue);
                writer.Write(longValue);
                break;
            // Используем decimal и записываем 128 бит (16 байт)
            case <= 29:
                // Получаем биты decimal без выделения массива
                Decimal128Bits bits = new Decimal128Bits(scaledValue);

                // Записываем 16 байт в порядке little-endian
                writer.Write(bits.Low64);  // Нижние 64 бита
                writer.Write(bits.High64); // Верхние 64 бита
                break;
            default:
                throw new NotSupportedException("Превышена максимальная точность для данного метода.");
        }
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

    private int GetSizeFromPrecision(int precision)
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
        var byteCount = value.GetByteCount();
        Span<byte> buffer = stackalloc byte[byteCount];
        value.TryWriteBytes(buffer, out _);
        Span<byte> decimalBytes = stackalloc byte[Size];

        if (buffer.Length > Size)
            throw new OverflowException($"Trying to write {buffer.Length} bytes, at most {Size} expected");

        buffer.CopyTo(decimalBytes);

        // If a negative BigInteger is not long enough to fill the whole buffer,
        // the remainder needs to be filled with 0xFF
        if (value.Sign < 0)
        {
            decimalBytes.Fill(0xFF);
        }
        writer.Write(decimalBytes);
    }

    private decimal ScaleDecimal(decimal value, int scale)
    {
        // Умножаем на 10^scale без использования Math.Pow и выделения памяти
        decimal scaleFactor = ScaleFactors[scale];
        return value * scaleFactor;
    }

    private static readonly decimal[] ScaleFactors = new decimal[]
    {
        1m,                      // 10^0
        10m,                     // 10^1
        100m,                    // 10^2
        1000m,                   // 10^3
        10000m,                  // 10^4
        100000m,                 // 10^5
        1000000m,                // 10^6
        10000000m,               // 10^7
        100000000m,              // 10^8
        1000000000m,             // 10^9
        10000000000m,            // 10^10
        100000000000m,           // 10^11
        1000000000000m,          // 10^12
        10000000000000m,         // 10^13
        100000000000000m,        // 10^14
        1000000000000000m,       // 10^15
        10000000000000000m,      // 10^16
        100000000000000000m,     // 10^17
        1000000000000000000m,    // 10^18
        10000000000000000000m,   // 10^19
        100000000000000000000m,  // 10^20
        1000000000000000000000m, // 10^21
        10000000000000000000000m, // 10^22
        100000000000000000000000m, // 10^23
        1000000000000000000000000m, // 10^24
        10000000000000000000000000m, // 10^25
        100000000000000000000000000m, // 10^26
        1000000000000000000000000000m, // 10^27
        10000000000000000000000000000m // 10^28
    };
}
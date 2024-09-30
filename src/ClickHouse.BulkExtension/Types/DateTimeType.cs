using System.Runtime.CompilerServices;
using ClickHouse.BulkExtension.Annotation;

namespace ClickHouse.BulkExtension.Types;

class DateTimeType<T>
    where T : struct
{
    public static readonly DateTimeType<T> DateTime64Second = new(DateTimePrecision.Second);
    public static readonly DateTimeType<T> DateTime64Millisecond = new(DateTimePrecision.Millisecond);
    public static readonly DateTimeType<T> DateTime64Microsecond = new(DateTimePrecision.Microsecond);
    public static readonly DateTimeType<T> DateTime64Nanosecond = new(DateTimePrecision.Nanosecond);

    private readonly byte _precision;

    private DateTimeType(DateTimePrecision precision)
    {
        _precision = (byte)precision;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Write(Memory<byte> buffer, T value)
    {
        var dateTimeOffset = ToDateTimeOffset(value);

        var multiplier = ScaleFactors[_precision];

        // Get Unix-time in seconds and multiply it
        var unixTimeSeconds = dateTimeOffset.ToUnixTimeSeconds();
        var unixTimeScaled = unixTimeSeconds * multiplier;

        // Calculate the fractional part of seconds and scale it
        var fractionalTicks = dateTimeOffset.Ticks % TimeSpan.TicksPerSecond;
        var fractional = fractionalTicks * multiplier / TimeSpan.TicksPerSecond;

        var unixTime = unixTimeScaled + fractional;

        return Int64Type.Instance.Write(buffer, unixTime);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private DateTimeOffset ToDateTimeOffset(T value)
    {
        return value switch
        {
            DateTimeOffset v => v,
            DateTime dt      => new DateTimeOffset(dt),
            DateOnly date    => new DateTimeOffset(date.Year, date.Month, date.Day, 0, 0, 0, TimeSpan.Zero),
            _                => throw new NotSupportedException()
        };
    }
    
    private static readonly long[] ScaleFactors = new long[]
    {
        1L,                      // 10^0
        10L,                     // 10^1
        100L,                    // 10^2
        1000L,                   // 10^3
        10000L,                  // 10^4
        100000L,                 // 10^5
        1000000L,                // 10^6
        10000000L,               // 10^7
        100000000L,              // 10^8
        1000000000L,             // 10^9
    };
}
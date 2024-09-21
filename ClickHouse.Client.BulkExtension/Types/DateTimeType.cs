using System.Reflection;
using ClickHouse.Client.BulkExtension.Annotation;

namespace ClickHouse.Client.BulkExtension.Types;

class DateTimeType<T>
    where T : struct
{
    public static readonly MethodInfo WriteMethod = typeof(DateTimeType<T>).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly DateTimeType<T> DateTime64Second = new(DateTimePrecision.Second);
    public static readonly DateTimeType<T> DateTime64Millisecond = new(DateTimePrecision.Millisecond);
    public static readonly DateTimeType<T> DateTime64Microsecond = new(DateTimePrecision.Microsecond);
    public static readonly DateTimeType<T> DateTime64Nanosecond = new(DateTimePrecision.Nanosecond);

    private readonly byte _precision;

    public DateTimeType(DateTimePrecision precision)
    {
        _precision = (byte)precision;
    }

    public void Write(BinaryWriter writer, T value)
    {
        var dateTimeOffset = ToDateTimeOffset(value);

        // Вычисляем множитель для precision
        var multiplier = GetDateTime64Multiplier();

        // Получаем Unix-время в секундах и умножаем на множитель
        var unixTimeSeconds = dateTimeOffset.ToUnixTimeSeconds();
        var unixTimeScaled = unixTimeSeconds * multiplier;

        // Вычисляем дробную часть секунд и масштабируем ее
        var fractionalTicks = dateTimeOffset.Ticks % TimeSpan.TicksPerSecond;
        var fractional = fractionalTicks * multiplier / TimeSpan.TicksPerSecond;

        // Итоговое значение времени
        var unixTime = unixTimeScaled + fractional;

        // Записываем значение как Int64 в порядке Little-Endian
        writer.Write(unixTime);
    }

    private long GetDateTime64Multiplier()
    {
        long multiplier = 1;
        for (int i = 0; i < _precision; i++)
        {
            multiplier *= 10;
        }
        return multiplier;
    }

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
}
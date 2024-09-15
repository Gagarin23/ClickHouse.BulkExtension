using System.Reflection;
using ClickHouse.Client.BulkExtension.Types.Base;
using NodaTime;

namespace ClickHouse.Client.BulkExtension.Types;

class DateTimeType<T> : AbstractDateTimeType
    where T : struct
{
    public static readonly MethodInfo WriteMethod = typeof(DateTimeType<T>).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;

    public int Scale { get; }

    public DateTimeType(int scale)
    {
        Scale = scale;
    }

    public virtual void Write(BinaryWriter writer, T value)
    {
        writer.Write(ToClickHouseTicks(Instant.FromDateTimeOffset(CoerceToDateTimeOffset(value))));
    }

    private long ToClickHouseTicks(Instant instant)
    {
        return ShiftDecimalPlaces(instant.ToUnixTimeTicks(), Scale - 7);
    }

    private long ShiftDecimalPlaces(long value, int places)
    {
        if (places == 0)
        {
            return value;
        }

        var factor = ToPower(10, Math.Abs(places));
        return places < 0 ? value / factor : value * factor;
    }

    private long ToPower(int value, int power)
    {
        checked
        {
            long result = 1;
            while (power > 0)
            {
                if ((power & 1) == 1)
                {
                    result *= value;
                }

                power >>= 1;
                if (power <= 0)
                {
                    break;
                }

                value *= value;
            }
            return result;
        }
    }
}
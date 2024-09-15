using NodaTime;

namespace ClickHouse.Client.BulkExtension.Types.Base;

abstract class AbstractDateTimeType
{
    public DateTimeZone TimeZone { get; set; }

    public DateTimeZone TimeZoneOrUtc => TimeZone ?? DateTimeZone.Utc;
    public DateTimeOffset CoerceToDateTimeOffset<T>(T value) where T : struct
    {
        return value switch
        {
            DateTimeOffset v => v,
            DateTime dt      => TimeZoneOrUtc.AtLeniently(LocalDateTime.FromDateTime(dt)).ToDateTimeOffset(),
            DateOnly date    => new DateTimeOffset(date.Year, date.Month, date.Day, 0, 0, 0, TimeSpan.Zero),
            OffsetDateTime o => o.ToDateTimeOffset(),
            ZonedDateTime z  => z.ToDateTimeOffset(),
            Instant i        => ToDateTimeOffset(i),
            _                => throw new NotSupportedException()
        };
    }

    private DateTimeOffset ToDateTimeOffset(Instant instant)
    {
        return instant.InZone(TimeZoneOrUtc).ToDateTimeOffset();
    }
}
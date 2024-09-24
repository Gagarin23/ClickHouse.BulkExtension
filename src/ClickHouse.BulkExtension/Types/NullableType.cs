namespace ClickHouse.BulkExtension.Types;

static class NullableType
{
    public static int WriteFlag<T>(Memory<byte> buffer, T? value) where T : struct
    {
        return BooleanType.Instance.Write(buffer, value.HasValue);
    }
}
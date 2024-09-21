namespace ClickHouse.Client.BulkExtension.Types;

static class NullableType
{
    public static void WriteFlag<T>(BinaryWriter writer, T? value) where T : struct
    {
        writer.Write(value.HasValue);
    }
}
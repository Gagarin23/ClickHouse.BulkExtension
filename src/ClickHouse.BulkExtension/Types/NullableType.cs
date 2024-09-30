using System.Runtime.CompilerServices;

namespace ClickHouse.BulkExtension.Types;

static class NullableType
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteFlag<T>(Memory<byte> buffer, T? value) where T : struct
    {
        return BooleanType.Instance.Write(buffer, value.HasValue);
    }
}
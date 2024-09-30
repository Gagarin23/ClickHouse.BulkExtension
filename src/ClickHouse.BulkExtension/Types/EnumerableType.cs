using System.Runtime.CompilerServices;

namespace ClickHouse.BulkExtension.Types;

static class EnumerableType<T>
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteCount(Memory<byte> buffer, IEnumerable<T>? enumerable)
    {
        if (enumerable is null)
        {
            return buffer.Write7BitEncodedInt(0);
        }

        var collection = enumerable as ICollection<T> ?? enumerable.ToList();
        return buffer.Write7BitEncodedInt(collection.Count);
    }
}
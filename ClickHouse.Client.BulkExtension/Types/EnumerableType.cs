namespace ClickHouse.Client.BulkExtension.Types;

static class EnumerableType<T>
{
    public static void WriteCount(BinaryWriter writer, IEnumerable<T>? enumerable)
    {
        if (enumerable is null)
        {
            writer.Write7BitEncodedInt(0);
            return;
        }

        var collection = enumerable as ICollection<T> ?? enumerable.ToList();
        writer.Write7BitEncodedInt(collection.Count);
    }
}
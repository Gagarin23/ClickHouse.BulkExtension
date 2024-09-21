namespace ClickHouse.Client.BulkExtension.Types;

static class MapType
{
    public static void WriteCount<TKey, TValue>(BinaryWriter writer, IDictionary<TKey, TValue>? dict)
    {
        if (dict is null)
        {
            writer.Write7BitEncodedInt(0);
            return;
        }

        writer.Write7BitEncodedInt(dict.Count);
    }
}
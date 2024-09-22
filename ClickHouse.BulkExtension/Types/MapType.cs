using System.Collections;

namespace ClickHouse.BulkExtension.Types;

class MapType
{
    public static readonly MapType Instance = new MapType();

    private MapType() { }

    public int WriteCount(Memory<byte> buffer, IDictionary? dict)
    {
        if (dict is null)
        {
            return buffer.Write7BitEncodedInt(0);
        }

        return buffer.Write7BitEncodedInt(dict.Count);
    }
}
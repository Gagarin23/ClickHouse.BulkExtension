namespace ClickHouse.Client.BulkExtension;

struct Key
{
    public static IEqualityComparer<Key> Comparer { get; } = new KeyEqualityComparer();

    public string TableName { get; }
    public IReadOnlyList<string> SortedColumnNames { get; }

    public Key(string tableName, IReadOnlyList<string> sortedColumnNames)
    {
        TableName = tableName;
        SortedColumnNames = sortedColumnNames;
    }

    private class KeyEqualityComparer : IEqualityComparer<Key>
    {
        public bool Equals(Key x, Key y) => x.TableName == y.TableName && x.SortedColumnNames.SequenceEqual(y.SortedColumnNames);
        public int GetHashCode(Key obj) => HashCode.Combine(obj.TableName, obj.SortedColumnNames);
    }
}


using System.Collections;
using ClickHouse.BulkExtension;
using ClickHouse.Client.ADO;

namespace ClickHouse.Client.BulkExtension;

public static class ClickHouseConnectionExtensions
{
    public static Task BulkCopyAsync<T>
    (
        this ClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columns,
        IEnumerable<T> source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        var copy = new ClickHouseCopy<T>(tableName, columns);
        return connection.PostStreamAsync(null, copy.GetStreamWriteCallBack(source, useCompression), useCompression, cancellationToken);
    }

    public static Task BulkCopyAsync<T>
    (
        this IClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columns,
        IEnumerable<T> source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        if (connection is not ClickHouseConnection c)
        {
            throw new ArgumentException("Connection must be of type ClickHouseConnection", nameof(connection));
        }

        return BulkCopyAsync(c, tableName, columns, source, useCompression, cancellationToken);
    }

    public static Task BulkCopyAsync<T>
    (
        this ClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columns,
        IAsyncEnumerable<T> source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        var copy = new ClickHouseAsyncCopy<T>(tableName, columns);
        return connection.PostStreamAsync(null, copy.GetStreamWriteCallBack(source, useCompression), useCompression, cancellationToken);
    }

    public static Task BulkCopyAsync<T>
    (
        this IClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columns,
        IAsyncEnumerable<T> source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        if (connection is not ClickHouseConnection c)
        {
            throw new ArgumentException("Connection must be of type ClickHouseConnection", nameof(connection));
        }

        return BulkCopyAsync(c, tableName, columns, source, useCompression, cancellationToken);
    }

    public static Task BulkCopyAsync
    (
        this ClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columns,
        IEnumerable source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        var copy = new ClickHouseCopy(tableName, columns, source);
        return connection.PostStreamAsync(null, copy.GetStreamWriteCallBack(useCompression), useCompression, cancellationToken);
    }

    public static Task BulkCopyAsync
    (
        this IClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columns,
        IEnumerable source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        if (connection is not ClickHouseConnection c)
        {
            throw new ArgumentException("Connection must be of type ClickHouseConnection", nameof(connection));
        }

        return BulkCopyAsync(c, tableName, columns, source, useCompression, cancellationToken);
    }
}
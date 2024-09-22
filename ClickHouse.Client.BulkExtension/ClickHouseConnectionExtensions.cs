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
        IReadOnlyList<string> columnNames,
        IEnumerable<T> source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        var copy = new ClickHouseCopy<T>(tableName, columnNames);
        return connection.PostStreamAsync(tableName, copy.GetStreamWriteCallBack(source, useCompression), useCompression, cancellationToken);
    }

    public static Task BulkCopyAsync<T>
    (
        this IClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columnNames,
        IEnumerable<T> source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        if (connection is not ClickHouseConnection c)
        {
            throw new ArgumentException("Connection must be of type ClickHouseConnection", nameof(connection));
        }

        return BulkCopyAsync(c, tableName, columnNames, source, useCompression, cancellationToken);
    }

    public static Task BulkCopyAsync<T>
    (
        this ClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columnNames,
        IAsyncEnumerable<T> source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        var copy = new ClickHouseAsyncCopy<T>(tableName, columnNames);
        return connection.PostStreamAsync(tableName, copy.GetStreamWriteCallBack(source, useCompression), useCompression, cancellationToken);
    }

    public static Task BulkCopyAsync<T>
    (
        this IClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columnNames,
        IAsyncEnumerable<T> source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        if (connection is not ClickHouseConnection c)
        {
            throw new ArgumentException("Connection must be of type ClickHouseConnection", nameof(connection));
        }

        return BulkCopyAsync(c, tableName, columnNames, source, useCompression, cancellationToken);
    }

    public static Task BulkCopyAsync
    (
        this ClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columnNames,
        IEnumerable source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        var copy = new ClickHouseCopy(tableName, columnNames, source);
        return connection.PostStreamAsync(tableName, copy.GetStreamWriteCallBack(useCompression), useCompression, cancellationToken);
    }

    public static Task BulkCopyAsync
    (
        this IClickHouseConnection connection,
        string tableName,
        IReadOnlyList<string> columnNames,
        IEnumerable source,
        bool useCompression,
        CancellationToken cancellationToken = default
    )
    {
        if (connection is not ClickHouseConnection c)
        {
            throw new ArgumentException("Connection must be of type ClickHouseConnection", nameof(connection));
        }

        return BulkCopyAsync(c, tableName, columnNames, source, useCompression, cancellationToken);
    }
}
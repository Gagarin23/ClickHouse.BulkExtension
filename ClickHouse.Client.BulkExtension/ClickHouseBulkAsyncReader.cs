using System.Collections.Concurrent;
using System.IO.Compression;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using ClickHouse.Client.BulkExtension.Observability;
using static DotNext.Metaprogramming.CodeGenerator;

namespace ClickHouse.Client.BulkExtension;

static class ClickHouseBulkAsyncReaderCacheHolder
{
    public static readonly ConcurrentDictionary<Key, Entry> WriteDelegates = new ConcurrentDictionary<Key, Entry>(Key.Comparer);

    internal struct Entry
    {
        public string Query { get; }
        public Delegate WriteFunction { get; }

        public Entry(string query, Delegate writeFunction)
        {
            Query = query;
            WriteFunction = writeFunction;
        }
    }
}

public class ClickHouseBulkAsyncReader<T>
{
    private readonly IAsyncEnumerable<T> _source;
    private readonly bool _useCompression;
    private readonly int _bufferSize;
    private readonly Func<ClickHouseWriter, IAsyncEnumerable<T>, Task> _writeFunction;
    private readonly string _query;

    public ClickHouseBulkAsyncReader(IAsyncEnumerable<T> source, IReadOnlyList<string> sortedColumnNames, string tableName, bool useCompression, int bufferSize = 4096)
    {
        if (string.IsNullOrEmpty(tableName))
        {
            throw new ArgumentException(nameof(tableName));
        }
        if (sortedColumnNames == null || sortedColumnNames.Count == 0)
        {
            throw new ArgumentException(nameof(sortedColumnNames));
        }
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _useCompression = useCompression;
        _bufferSize = bufferSize;

        var entry = ClickHouseBulkAsyncReaderCacheHolder.WriteDelegates.GetOrAdd(new Key(tableName, sortedColumnNames), GetEntry);
        _writeFunction = (Func<ClickHouseWriter, IAsyncEnumerable<T>, Task>)entry.WriteFunction;
        _query = entry.Query;
    }

    private ClickHouseBulkAsyncReaderCacheHolder.Entry GetEntry(Key key)
    {
        var query = $"INSERT INTO {key.TableName} ({string.Join(", ", key.SortedColumnNames.Select(x => $"`{x}`"))}) FORMAT RowBinary";
        var writeFunction = BuildWriteFunction(key.SortedColumnNames);

        return new ClickHouseBulkAsyncReaderCacheHolder.Entry(query, writeFunction);
    }

    public ClickHouseStreamContent GetStreamContent()
    {
        return new ClickHouseStreamContent(WriteAsync);
    }

    public Func<Stream, CancellationToken, Task> GetStreamWriteCallBack()
    {
        return WriteAsync;
    }

    private async Task WriteAsync(Stream stream, CancellationToken cancellationToken)
    {
        var targetStream = _useCompression
            ? new GZipStream(stream, CompressionLevel.Fastest, true)
            : stream;

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            await using (var sw = new StreamWriter(targetStream, Encoding.UTF8, _query.Length, true))
            {
                await sw.WriteLineAsync(_query);
            }
            await using var writer = new ClickHouseWriter(targetStream, _bufferSize);
            await _writeFunction(writer, _source);
        }
        catch (Exception e)
        {
            Events.Writer.Error($"{nameof(ClickHouseBulkReader)}<{_source.GetType().Name}>", e);
        }
        finally
        {
            await targetStream.DisposeAsync();
        }
    }

    private Func<ClickHouseWriter, IAsyncEnumerable<T>, Task> BuildWriteFunction(IReadOnlyList<string> sortedColumnNames)
    {
        var sortedProperties = StaticFunctions.GetSortedProperties(typeof(T), sortedColumnNames);
        var lambda = BuildLambda(sortedProperties);
        return lambda.Compile();
    }

    private Expression<Func<ClickHouseWriter, IAsyncEnumerable<T>, Task>> BuildLambda(PropertyInfo[] sortedProperties)
    {
        var lambda = AsyncLambda<Func<ClickHouseWriter, IAsyncEnumerable<T>, Task>>(fun =>
        {
            AwaitForEach(fun[1], row =>
            {
                foreach (var property in sortedProperties)
                {
                    var getter = property.GetMethod!;
                    var getExpression = Expression.Property(row, getter);
                    StaticFunctions.SetWriteExpression(property.PropertyType, fun[0], getExpression);
                }
            });

        });
        return lambda;
    }
}
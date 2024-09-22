using System.Collections.Concurrent;
using System.IO.Compression;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using ClickHouse.BulkExtension.Observability;
using static DotNext.Metaprogramming.CodeGenerator;

namespace ClickHouse.BulkExtension;

static class ClickHouseGenericCopyCacheHolder
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

public class ClickHouseCopy<T>
{
    private readonly Func<ClickHouseWriter, IEnumerable<T>, Task> _writeFunction;
    private readonly string _query;

    private IEnumerable<T> _source;
    private bool _useCompression;
    private int _bufferSize;

    public ClickHouseCopy(string tableName, IReadOnlyList<string> columnNames)
    {
        if (string.IsNullOrEmpty(tableName))
        {
            throw new ArgumentException(nameof(tableName));
        }
        if (columnNames == null || columnNames.Count == 0)
        {
            throw new ArgumentException(nameof(columnNames));
        }

        var entry = ClickHouseGenericCopyCacheHolder.WriteDelegates.GetOrAdd(new Key(tableName, columnNames), GetEntry);
        _writeFunction = (Func<ClickHouseWriter, IEnumerable<T>, Task>)entry.WriteFunction;
        _query = entry.Query;
    }

    public ClickHouseStreamContent GetStreamContent(IEnumerable<T> source, bool useCompression, int bufferSize = 4096)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _useCompression = useCompression;
        _bufferSize = bufferSize;
        return new ClickHouseStreamContent(WriteAsync);
    }

    public Func<Stream, CancellationToken, Task> GetStreamWriteCallBack(IEnumerable<T> source, bool useCompression, int bufferSize = 4096)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _useCompression = useCompression;
        _bufferSize = bufferSize;
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
            Events.Writer.Error($"{nameof(ClickHouseCopy)}<{_source.GetType().Name}>", e);
        }
        finally
        {
            await targetStream.DisposeAsync();
        }
    }

    private ClickHouseGenericCopyCacheHolder.Entry GetEntry(Key key)
    {
        var query = $"INSERT INTO {key.TableName} ({string.Join(", ", key.SortedColumnNames.Select(x => $"`{x}`"))}) FORMAT RowBinary";
        var writeFunction = BuildWriteFunction(key.SortedColumnNames);

        return new ClickHouseGenericCopyCacheHolder.Entry(query, writeFunction);
    }

    private Func<ClickHouseWriter, IEnumerable<T>, Task> BuildWriteFunction(IReadOnlyList<string> sortedColumnNames)
    {
        var sortedProperties = StaticFunctions.GetSortedProperties(typeof(T), sortedColumnNames);
        var lambda = BuildLambda(sortedProperties);
        return lambda.Compile();
    }

    private Expression<Func<ClickHouseWriter, IEnumerable<T>, Task>> BuildLambda(PropertyInfo[] sortedProperties)
    {
        var lambda = AsyncLambda<Func<ClickHouseWriter, IEnumerable<T>, Task>>(fun =>
        {
            ForEach(fun[1], row =>
            {
                var writerParameter = fun[0];
                foreach (var property in sortedProperties)
                {
                    var getter = property.GetMethod!;
                    var getExpression = Expression.Property(row, getter);
                    StaticFunctions.SetWriteExpression(property.PropertyType, writerParameter, getExpression);
                }
            });

        });
        return lambda;
    }
}
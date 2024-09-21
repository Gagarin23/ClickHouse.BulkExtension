using System.Collections.Concurrent;
using System.IO.Compression;
using System.IO.Pipelines;
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

public class ClickHouseBulkAsyncReader<T> : IAsyncDisposable
{
    private static readonly ConcurrentDictionary<Key, Func<BinaryWriter, IAsyncEnumerable<T>, Task>> WriteDelegates
        = new ConcurrentDictionary<Key, Func<BinaryWriter, IAsyncEnumerable<T>, Task>>(Key.Comparer);

    private readonly IAsyncEnumerable<T> _source;
    private readonly TaskCompletionSource _tcs;
    private readonly PipeReader _reader;
    private readonly PipeWriter _writer;
    private readonly Func<BinaryWriter, IAsyncEnumerable<T>, Task> _writeFunction;
    private readonly string _query;

    private Stream? _readStream;

    public ClickHouseBulkAsyncReader(IAsyncEnumerable<T> source, IReadOnlyList<string> sortedColumnNames, string tableName)
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

        var entry = ClickHouseBulkAsyncReaderCacheHolder.WriteDelegates.GetOrAdd(new Key(tableName, sortedColumnNames), GetEntry);
        _writeFunction = (Func<BinaryWriter, IAsyncEnumerable<T>, Task>)entry.WriteFunction;
        _query = entry.Query;

        var pipe = new Pipe();
        _reader = pipe.Reader;
        _writer = pipe.Writer;
        _tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private ClickHouseBulkAsyncReaderCacheHolder.Entry GetEntry(Key key)
    {
        var query = $"INSERT INTO {key.TableName} ({string.Join(", ", key.SortedColumnNames.Select(x => $"`{x}`"))}) FORMAT RowBinary";
        var writeFunction = BuildWriteFunction(key.SortedColumnNames);

        return new ClickHouseBulkAsyncReaderCacheHolder.Entry(query, writeFunction);
    }

    public Task CompleteAsync()
    {
        if (_readStream == null)
        {
            _tcs.SetResult();
        }
        return _tcs.Task;
    }

    public Stream GetStream(bool useCompression)
    {
        if (_readStream != null)
        {
            return _readStream;
        }
        _readStream = _reader.AsStream();
        _ = Task.Run(() => WriteAsync(useCompression));
        return _readStream;
    }

    private async Task WriteAsync(bool useCompression)
    {
        var targetStream = useCompression
            ? new GZipStream(_writer.AsStream(), CompressionLevel.Fastest, false)
            : _writer.AsStream();

        var binaryWriter = new BinaryWriter(targetStream, Encoding.UTF8, false);
        try
        {
            await using (var sw = new StreamWriter(targetStream, Encoding.UTF8, _query.Length, true))
            {
                await sw.WriteLineAsync(_query);
            }
            await _writeFunction(binaryWriter, _source);
            _tcs.SetResult();
        }
        catch (Exception e)
        {
            Events.Writer.Error($"{nameof(ClickHouseBulkAsyncReader<T>)}<{typeof(T).Name}>", e);
            _tcs.SetException(e);
        }
        finally
        {
            await binaryWriter.DisposeAsync();
            await _writer.CompleteAsync();
        }
    }

    private Func<BinaryWriter, IAsyncEnumerable<T>, Task> BuildWriteFunction(IReadOnlyList<string> sortedColumnNames)
    {
        var sortedProperties = StaticFunctions.GetSortedProperties(typeof(T), sortedColumnNames);
        var lambda = BuildLambda(sortedProperties);
        return lambda.Compile();
    }

    private Expression<Func<BinaryWriter, IAsyncEnumerable<T>, Task>> BuildLambda(PropertyInfo[] sortedProperties)
    {
        var lambda = AsyncLambda<Func<BinaryWriter, IAsyncEnumerable<T>, Task>>(fun =>
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

    public async ValueTask DisposeAsync()
    {
        await CompleteAsync();
    }
}
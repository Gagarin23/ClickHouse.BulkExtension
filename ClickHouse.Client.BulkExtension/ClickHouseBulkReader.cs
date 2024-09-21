using System.Collections;
using System.Collections.Concurrent;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using ClickHouse.Client.BulkExtension.Observability;
using static DotNext.Metaprogramming.CodeGenerator;

namespace ClickHouse.Client.BulkExtension;

public class ClickHouseBulkReader : IAsyncDisposable
{
    private static readonly ConcurrentDictionary<Key, Entry> WriteDelegates = new ConcurrentDictionary<Key, Entry>(Key.Comparer);

    private readonly IEnumerable _source;
    private readonly Type _elementType;
    private readonly TaskCompletionSource _tcs;
    private readonly PipeReader _reader;
    private readonly PipeWriter _writer;
    private readonly Action<BinaryWriter, IEnumerable> _writeFunction;
    private readonly string _query;

    private bool _isTypeDeclared;
    private Stream? _readStream;

    public ClickHouseBulkReader(IEnumerable source, IReadOnlyList<string> sortedColumnNames, string tableName)
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
        _elementType = GetElementType(source) ?? throw new ArgumentException("Could not determine element type of source");

        var entry = WriteDelegates.GetOrAdd(new Key(tableName, sortedColumnNames), GetEntry);
        _writeFunction = entry.WriteFunction;
        _query = entry.Query;

        var pipe = new Pipe();
        _reader = pipe.Reader;
        _writer = pipe.Writer;
        _tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public Task CompleteAsync()
    {
        if (_readStream == null)
        {
            _tcs.SetResult();
        }
        return _tcs.Task;
    }

    private Type? GetElementType(IEnumerable source)
    {
        var elementType = source.GetType()
            .GetInterfaces()
            .FirstOrDefault(x => x.IsGenericType && x == typeof(IEnumerable<>).MakeGenericType(x.GenericTypeArguments))?
            .GenericTypeArguments[0];

        if (elementType != null)
        {
            _isTypeDeclared = true;
            return elementType;
        }

        var e = source.GetEnumerator();
        _isTypeDeclared = e.MoveNext();
        if (!_isTypeDeclared)
        {
            return null;
        }

        var current = e.Current;
        var rowType = current!.GetType();
        (e as IDisposable)?.Dispose();
        return rowType;
    }

    private Entry GetEntry(Key key)
    {
        var query = $"INSERT INTO {key.TableName} ({string.Join(", ", key.SortedColumnNames.Select(x => $"`{x}`"))}) FORMAT RowBinary";
        var writeFunction = BuildWriteFunction(key.SortedColumnNames);

        return new Entry(query, writeFunction);
    }

    public Stream GetStream(bool useCompression)
    {
        if (!_isTypeDeclared)
        {
            _tcs.SetResult();
            return Stream.Null;
        }
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
            _writeFunction(binaryWriter, _source);
            _tcs.SetResult();
        }
        catch (Exception e)
        {
            Events.Writer.Error($"{nameof(ClickHouseBulkReader)}<{_source.GetType().Name}>", e);
            _tcs.SetException(e);
        }
        finally
        {
            await binaryWriter.DisposeAsync();
            await _writer.CompleteAsync();
        }
    }

    private Action<BinaryWriter, IEnumerable> BuildWriteFunction(IReadOnlyList<string> sortedColumnNames)
    {
        var sortedProperties = StaticFunctions.GetSortedProperties(_elementType, sortedColumnNames);
        var lambda = BuildLambda(sortedProperties);
        return lambda.Compile();
    }

    private Expression<Action<BinaryWriter, IEnumerable>> BuildLambda(PropertyInfo[] sortedProperties)
    {
        var lambda = Lambda<Action<BinaryWriter, IEnumerable>>(fun =>
        {
            var rowsParameter = fun[1];
            ParameterExpression rowsVar = rowsParameter;
            var targetTypeEnumerable = typeof(IEnumerable<>).MakeGenericType(_elementType);
            var isGenericEnumerable = _source.GetType().GetInterfaces().Any(x => x == targetTypeEnumerable);
            if (isGenericEnumerable)
            {
                var castExpression = Expression.Convert(rowsParameter, targetTypeEnumerable);
                rowsVar = DeclareVariable(targetTypeEnumerable, "rows");
                Assign(rowsVar, castExpression);
            }

            ForEach(rowsVar, row =>
            {
                var binaryWriterParameter = fun[0];
                Expression localRow = row;
                if (!isGenericEnumerable)
                {
                    var castExpression = Expression.Convert(row, _elementType);
                    var rowVar = DeclareVariable(_elementType, "row");
                    Assign(rowVar, castExpression);
                    localRow = rowVar;
                }
                foreach (var property in sortedProperties)
                {
                    var getter = property.GetMethod!;
                    var getExpression = Expression.Property(localRow, getter);
                    StaticFunctions.SetWriteExpression(property.PropertyType, binaryWriterParameter, getExpression);
                }
            });

        });
        return lambda;
    }

    public async ValueTask DisposeAsync()
    {
        await CompleteAsync();
    }

    private struct Entry
    {
        public string Query { get; }
        public Action<BinaryWriter, IEnumerable> WriteFunction { get; }

        public Entry(string query, Action<BinaryWriter, IEnumerable> writeFunction)
        {
            Query = query;
            WriteFunction = writeFunction;
        }
    }
}
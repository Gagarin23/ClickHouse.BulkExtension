using System.Collections;
using System.Collections.Concurrent;
using System.IO.Compression;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using ClickHouse.BulkExtension.Observability;
using static DotNext.Metaprogramming.CodeGenerator;

namespace ClickHouse.BulkExtension;

public class ClickHouseCopy
{
    private static readonly ConcurrentDictionary<Key, Entry> WriteDelegates = new ConcurrentDictionary<Key, Entry>(Key.Comparer);

    private readonly Func<ClickHouseWriter, IEnumerable, Task> _writeFunction;
    private readonly string _query;
    private readonly IEnumerable _source;

    private Type _elementType;
    private bool _useCompression;
    private int _bufferSize;

    public ClickHouseCopy(string tableName, IReadOnlyList<string> columnNames, IEnumerable source)
    {
        if (string.IsNullOrEmpty(tableName))
        {
            throw new ArgumentException(nameof(tableName));
        }
        if (columnNames == null || columnNames.Count == 0)
        {
            throw new ArgumentException(nameof(columnNames));
        }
        _source = source;
        var entry = WriteDelegates.GetOrAdd(new Key(tableName, columnNames), GetEntry);
        _writeFunction = entry.WriteFunction;
        _query = entry.Query;
    }

    public ClickHouseStreamContent GetStreamContent(bool useCompression, int bufferSize = 4096)
    {
        _useCompression = useCompression;
        _bufferSize = bufferSize;
        return new ClickHouseStreamContent(WriteAsync);
    }

    public Func<Stream, CancellationToken, Task> GetStreamWriteCallBack(bool useCompression, int bufferSize = 4096)
    {
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

    private Type? GetElementType(IEnumerable source)
    {
        var elementType = source.GetType()
            .GetInterfaces()
            .FirstOrDefault(x => x.IsGenericType && x == typeof(IEnumerable<>).MakeGenericType(x.GenericTypeArguments))?
            .GenericTypeArguments[0];

        if (elementType != null)
        {
            return elementType;
        }

        var e = source.GetEnumerator();
        var isEmpty = !e.MoveNext();
        if (isEmpty)
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

    private Func<ClickHouseWriter, IEnumerable, Task> BuildWriteFunction(IReadOnlyList<string> sortedColumnNames)
    {
        _elementType ??= GetElementType(_source) ?? throw new ArgumentException("Could not determine element type of source");
        var sortedProperties = StaticFunctions.GetSortedProperties(_elementType, sortedColumnNames);
        var lambda = BuildLambda(sortedProperties);
        return lambda.Compile();
    }

    private Expression<Func<ClickHouseWriter, IEnumerable, Task>> BuildLambda(PropertyInfo[] sortedProperties)
    {
        var lambda = AsyncLambda<Func<ClickHouseWriter, IEnumerable, Task>>(fun =>
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
                var writerParameter = fun[0];
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
                    StaticFunctions.SetWriteExpression(property.PropertyType, writerParameter, getExpression);
                }
            });

        });
        return lambda;
    }

    private struct Entry
    {
        public string Query { get; }
        public Func<ClickHouseWriter, IEnumerable, Task> WriteFunction { get; }

        public Entry(string query, Func<ClickHouseWriter, IEnumerable, Task> writeFunction)
        {
            Query = query;
            WriteFunction = writeFunction;
        }
    }
}
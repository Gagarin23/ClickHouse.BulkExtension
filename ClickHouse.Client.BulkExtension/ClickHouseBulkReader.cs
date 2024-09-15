using System.Buffers;
using System.Collections;
using System.Collections.Concurrent;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using ClickHouse.Client.BulkExtension.Annotation;
using ClickHouse.Client.BulkExtension.Numerics;
using ClickHouse.Client.BulkExtension.Types;
using NodaTime;
using static DotNext.Metaprogramming.CodeGenerator;

namespace ClickHouse.Client.BulkExtension;

public class ClickHouseBulkReader
{
    private static readonly ConcurrentDictionary<Type, Action<BinaryWriter, IEnumerable>> WriteDelegates = new ConcurrentDictionary<Type, Action<BinaryWriter, IEnumerable>>();

    private readonly TaskCompletionSource _tcs;
    private readonly PipeReader _reader;
    private readonly IReadOnlyList<string> _sortedColumnNames;
    private readonly IEnumerable _source;
    private readonly string _tableName;
    private readonly PipeWriter _writer;

    private BinaryWriter _binaryWriter;
    private Stream? _stream;

    public ClickHouseBulkReader(IEnumerable source, IReadOnlyList<string> sortedColumnNames, string tableName)
    {
        _sortedColumnNames = sortedColumnNames;
        _tableName = tableName;
        _source = source;
        var pipe = new Pipe();
        _reader = pipe.Reader;
        _writer = pipe.Writer;
        _tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public async Task<Stream> GetStreamAsync(bool isCompressed, CancellationToken cancellationToken = default)
    {
        if (_stream != null)
            return _stream;

        _binaryWriter = isCompressed
            ? new BinaryWriter(new GZipStream(_writer.AsStream(), CompressionLevel.Fastest, false), Encoding.UTF8, false)
            : new BinaryWriter(_writer.AsStream(), Encoding.UTF8, false);

        _stream = _reader.AsStream();

        _ = Task.Run(() => WriteAsync(cancellationToken), cancellationToken);
        await _tcs.Task;

        return _stream;
    }

    private async Task WriteAsync(CancellationToken cancellationToken)
    {
        var e = _source.GetEnumerator();
        if (!e.MoveNext())
        {
            await _writer.CompleteAsync();
            return;
        }

        var current = e.Current;
        var rowType = current.GetType();
        Action<BinaryWriter, IEnumerable> writeFunction;
        try
        {
            writeFunction = WriteDelegates.GetOrAdd(rowType, BuildWriteFunction);
            _tcs.SetResult();
        }
        catch (Exception exception)
        {
            _tcs.SetException(exception);
            return;
        }

        try
        {
            var query = $"INSERT INTO {_tableName} ({string.Join(", ", _sortedColumnNames)}) FORMAT RowBinary{Environment.NewLine}";
            _writer.Write(Encoding.UTF8.GetBytes(query));
            writeFunction(_binaryWriter, _source);
        }
        finally
        {
            await _writer.FlushAsync(cancellationToken);
            await _writer.CompleteAsync();
        }
    }

    private Action<BinaryWriter, IEnumerable> BuildWriteFunction(Type rowType)
    {
        var properties = rowType.GetProperties();
        var sortedProperties = new PropertyInfo[_sortedColumnNames.Count];

        for (var i = 0; i < _sortedColumnNames.Count; i++)
        {
            var columnName = _sortedColumnNames[i];
            PropertyInfo found = null!;
            foreach (var property in properties)
            {
                if (property.Name == columnName)
                {
                    found = property;
                    break;
                }
            }
            if (found == null || found.GetMethod == null)
            {
                throw new InvalidOperationException($"Property {columnName} not found in {rowType}");
            }
            sortedProperties[i] = found;
        }

        var lambda = Lambda<Action<BinaryWriter, IEnumerable>>(fun =>
        {
            var rowsParameter = fun[1];
            var targetTypeEnumerable = typeof(IEnumerable<>).MakeGenericType(rowType);
            var castExpression = Expression.Convert(rowsParameter, targetTypeEnumerable);
            var rowsVar = DeclareVariable(targetTypeEnumerable, "rows");
            Assign(rowsVar, castExpression);
            ForEach(rowsVar, row =>
            {
                var binaryWriterParameter = fun[0];
                foreach (var property in sortedProperties)
                {
                    var getter = property.GetMethod!;
                    var getExpression = Expression.Property(row, getter);
                    SetWriteExpression(property.PropertyType, binaryWriterParameter, getExpression);
                }
            });

        });

        return lambda.Compile();
    }

    private static void SetWriteExpression(Type elementType, ParameterExpression binaryWriterParameter, MemberExpression getExpression)
    {
        Expression writeExpression;
        if (elementType == typeof(Guid))
        {
            writeExpression = Expression.Call(Expression.Constant(UuidType.Instance), UuidType.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(bool))
        {
            writeExpression = Expression.Call(Expression.Constant(BooleanType.Instance), BooleanType.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(byte))
        {
            writeExpression = Expression.Call(Expression.Constant(UInt8Type.Instance), UInt8Type.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(short))
        {
            writeExpression = Expression.Call(Expression.Constant(Int16Type.Instance), Int16Type.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(int))
        {
            writeExpression = Expression.Call(Expression.Constant(Int32Type.Instance), Int32Type.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(long))
        {
            writeExpression = Expression.Call(Expression.Constant(Int64Type.Instance), Int64Type.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(sbyte))
        {
            writeExpression = Expression.Call(Expression.Constant(Int8Type.Instance), Int8Type.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(ushort))
        {
            writeExpression = Expression.Call(Expression.Constant(UInt16Type.Instance), UInt16Type.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(uint))
        {
            writeExpression = Expression.Call(Expression.Constant(UInt32Type.Instance), UInt32Type.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(ulong))
        {
            writeExpression = Expression.Call(Expression.Constant(UInt64Type.Instance), UInt64Type.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(float))
        {
            writeExpression = Expression.Call(Expression.Constant(Float32Type.Instance), Float32Type.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(double))
        {
            writeExpression = Expression.Call(Expression.Constant(Float64Type.Instance), Float64Type.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(decimal))
        {
            var precision = getExpression.Member.GetCustomAttribute<PrecisionAttribute>()?.Value ?? 18;
            var scale = getExpression.Member.GetCustomAttribute<ScaleAttribute>()?.Value ?? 6;
            var decimalType = new DecimalType(precision, scale);
            writeExpression = Expression.Call(Expression.Constant(decimalType), DecimalType.DecimalWriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(ClickHouseDecimal))
        {
            var precision = getExpression.Member.GetCustomAttribute<PrecisionAttribute>()?.Value ?? 18;
            var scale = getExpression.Member.GetCustomAttribute<ScaleAttribute>()?.Value ?? 6;
            var decimalType = new DecimalType(precision, scale);
            writeExpression = Expression.Call(Expression.Constant(decimalType), DecimalType.ClickHouseDecimalWriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(string))
        {
            writeExpression = Expression.Call(Expression.Constant(StringType.Instance), StringType.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType.IsEnum)
        {
            var size = Enum.GetValues(elementType).Length;
            Expression convertExpression;
            switch (size)
            {
                case <= byte.MaxValue:
                    convertExpression = Expression.Convert(getExpression, typeof(byte));
                    writeExpression = Expression.Call(Expression.Constant(UInt8Type.Instance), UInt8Type.WriteMethod, binaryWriterParameter, convertExpression);
                    break;
                case <= short.MaxValue:
                    convertExpression = Expression.Convert(getExpression, typeof(short));
                    writeExpression = Expression.Call(Expression.Constant(UInt16Type.Instance), UInt16Type.WriteMethod, binaryWriterParameter, convertExpression);
                    break;
                default:
                    convertExpression = Expression.Convert(getExpression, typeof(int));
                    writeExpression = Expression.Call(Expression.Constant(UInt32Type.Instance), UInt32Type.WriteMethod, binaryWriterParameter, convertExpression);
                    break;
            }
        }
        else if (elementType == typeof(DateTime))
        {
            var scale = getExpression.Member.GetCustomAttribute<ScaleAttribute>()?.Value ?? 3;
            var dateTimeType = new DateTimeType<DateTime>(scale);
            writeExpression = Expression.Call(Expression.Constant(dateTimeType), DateTimeType<DateTime>.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(DateTimeOffset))
        {
            var scale = getExpression.Member.GetCustomAttribute<ScaleAttribute>()?.Value ?? 3;
            var dateTimeType = new DateTimeType<DateTimeOffset>(scale);
            writeExpression = Expression.Call(Expression.Constant(dateTimeType), DateTimeType<DateTimeOffset>.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(DateOnly))
        {
            var scale = getExpression.Member.GetCustomAttribute<ScaleAttribute>()?.Value ?? 3;
            var dateTimeType = new DateTimeType<DateOnly>(scale);
            writeExpression = Expression.Call(Expression.Constant(dateTimeType), DateTimeType<DateOnly>.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(OffsetDateTime))
        {
            var scale = getExpression.Member.GetCustomAttribute<ScaleAttribute>()?.Value ?? 3;
            var dateTimeType = new DateTimeType<OffsetDateTime>(scale);
            writeExpression = Expression.Call(Expression.Constant(dateTimeType), DateTimeType<OffsetDateTime>.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(ZonedDateTime))
        {
            var scale = getExpression.Member.GetCustomAttribute<ScaleAttribute>()?.Value ?? 3;
            var dateTimeType = new DateTimeType<ZonedDateTime>(scale);
            writeExpression = Expression.Call(Expression.Constant(dateTimeType), DateTimeType<ZonedDateTime>.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(Instant))
        {
            var scale = getExpression.Member.GetCustomAttribute<ScaleAttribute>()?.Value ?? 3;
            var dateTimeType = new DateTimeType<Instant>(scale);
            writeExpression = Expression.Call(Expression.Constant(dateTimeType), DateTimeType<Instant>.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(IPAddress))
        {
            writeExpression = Expression.Call(Expression.Constant(IPType.Instance), IPType.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType.GetInterfaces().Any(x => x == typeof(IDictionary) && x.IsGenericType))
        {
            var keyType = elementType.GetGenericArguments()[0];
            var valueType = elementType.GetGenericArguments()[1];
            var writeMethod = typeof(EnumerableType<>).MakeGenericType(valueType).GetMethod("WriteCount", BindingFlags.Public | BindingFlags.Static);
            writeExpression = Expression.Call(writeMethod, binaryWriterParameter, getExpression);
            Statement(writeExpression);
            ForEach(getExpression, row =>
            {
                SetWriteExpression(keyType, binaryWriterParameter, row);
                SetWriteExpression(valueType, binaryWriterParameter, row);
            });
            return;
        }
        else if (elementType.GetInterfaces().Any(x => x == typeof(IEnumerable) && x.IsGenericType))
        {
            var genericType = elementType.GetGenericArguments()[0];
            var writeMethod = typeof(EnumerableType<>).MakeGenericType(genericType).GetMethod("WriteCount", BindingFlags.Public | BindingFlags.Static)!;
            writeExpression = Expression.Call(writeMethod, binaryWriterParameter, getExpression);
            Statement(writeExpression);
            ForEach(getExpression, row =>
            {
                SetWriteExpression(genericType, binaryWriterParameter, row);
            });
            return;
        }
        else if (elementType == typeof(ITuple))
        {
            var genericTypes = elementType.GetGenericArguments();
            var isValueTuple = !elementType.IsClass;
            for (int i = 0; i < genericTypes.Length; i++)
            {
                MemberExpression memberExpression;
                if (isValueTuple)
                {
                    var field = elementType.GetField($"Item{i + 1}")!;
                    memberExpression = Expression.Field(getExpression, field);
                }
                else
                {
                    var property = elementType.GetProperty($"Item{i + 1}")!;
                    memberExpression = Expression.Property(getExpression, property);
                }
                SetWriteExpression(genericTypes[i], binaryWriterParameter, memberExpression);
            }
            return;
        }
        else
        {
            throw new NotSupportedException($"Type {elementType} is not supported");
        }

        Statement(writeExpression);
    }
}
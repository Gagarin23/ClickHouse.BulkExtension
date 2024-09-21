using System.Collections;
using System.Linq.Expressions;
using System.Net;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using ClickHouse.Client.BulkExtension.Annotation;
using ClickHouse.Client.BulkExtension.Numerics;
using ClickHouse.Client.BulkExtension.Types;
using static DotNext.Metaprogramming.CodeGenerator;

namespace ClickHouse.Client.BulkExtension;

static class StaticFunctions
{
    public static PropertyInfo[] GetSortedProperties(Type rowType, IReadOnlyList<string> sortedColumnNames)
    {
        var properties = rowType.GetProperties();
        var sortedProperties = new PropertyInfo[sortedColumnNames.Count];

        for (var i = 0; i < sortedColumnNames.Count; i++)
        {
            var columnName = sortedColumnNames[i];
            PropertyInfo found = null!;
            foreach (var property in properties)
            {
                var propertyName = property.GetCustomAttribute<ClickHouseColumnAttribute>()?.Name ?? property.Name;
                if (propertyName == columnName)
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
        return sortedProperties;
    }

    public static void SetWriteExpression(Type elementType, ParameterExpression binaryWriterParameter, MemberExpression getExpression)
    {
        Expression writeExpression;

        if (elementType.IsValueType && elementType == typeof(Nullable<>).MakeGenericType(elementType))
        {
            var writeMethod = typeof(NullableType).GetMethod(nameof(NullableType.WriteFlag), BindingFlags.Public | BindingFlags.Static)!.MakeGenericMethod(elementType);
            Statement(Expression.Call(writeMethod, binaryWriterParameter, getExpression));
        }

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
        else if (elementType == typeof(BigInteger))
        {
            var bitsQuantity = getExpression.Member.GetCustomAttribute<ClickHouseColumnAttribute>()?.BigIntegerBits ?? BigIntegerBits.Bits128;
            var instance = bitsQuantity switch
            {
                BigIntegerBits.Bits128 => BigIntegerType.Instance128,
                BigIntegerBits.Bits256 => BigIntegerType.Instance256,
                _ => throw new NotSupportedException($"BigIntegerBits {bitsQuantity} is not supported")
            };
            writeExpression = Expression.Call(Expression.Constant(instance), BigIntegerType.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(decimal))
        {
            var columnAttribute = getExpression.Member.GetCustomAttribute<ClickHouseColumnAttribute>();
            var precision = columnAttribute?.Precision ?? 16;
            var scale = columnAttribute?.Scale ?? 4;
            var decimalType = new DecimalType(precision, scale);
            writeExpression = Expression.Call(Expression.Constant(decimalType), DecimalType.DecimalWriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(ClickHouseDecimal))
        {
            var columnAttribute = getExpression.Member.GetCustomAttribute<ClickHouseColumnAttribute>();
            var precision = columnAttribute?.Precision ?? 16;
            var scale = columnAttribute?.Scale ?? 4;
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
            var precision = getExpression.Member.GetCustomAttribute<ClickHouseColumnAttribute>()?.DateTimePrecision ?? DateTimePrecision.Second;
            var dateTimeType = precision switch
            {
                DateTimePrecision.Second => DateTimeType<DateTime>.DateTime64Second,
                DateTimePrecision.Millisecond => DateTimeType<DateTime>.DateTime64Millisecond,
                DateTimePrecision.Microsecond => DateTimeType<DateTime>.DateTime64Microsecond,
                DateTimePrecision.Nanosecond => DateTimeType<DateTime>.DateTime64Nanosecond,
            };
            writeExpression = Expression.Call(Expression.Constant(dateTimeType), DateTimeType<DateTime>.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(DateTimeOffset))
        {
            var precision = getExpression.Member.GetCustomAttribute<ClickHouseColumnAttribute>()?.DateTimePrecision ?? DateTimePrecision.Second;
            var dateTimeType = precision switch
            {
                DateTimePrecision.Second      => DateTimeType<DateTimeOffset>.DateTime64Second,
                DateTimePrecision.Millisecond => DateTimeType<DateTimeOffset>.DateTime64Millisecond,
                DateTimePrecision.Microsecond => DateTimeType<DateTimeOffset>.DateTime64Microsecond,
                DateTimePrecision.Nanosecond  => DateTimeType<DateTimeOffset>.DateTime64Nanosecond,
            };
            writeExpression = Expression.Call(Expression.Constant(dateTimeType), DateTimeType<DateTimeOffset>.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(DateOnly))
        {
            var precision = getExpression.Member.GetCustomAttribute<ClickHouseColumnAttribute>()?.DateTimePrecision ?? DateTimePrecision.Second;
            var dateTimeType = precision switch
            {
                DateTimePrecision.Second      => DateTimeType<DateOnly>.DateTime64Second,
                DateTimePrecision.Millisecond => DateTimeType<DateOnly>.DateTime64Millisecond,
                DateTimePrecision.Microsecond => DateTimeType<DateOnly>.DateTime64Microsecond,
                DateTimePrecision.Nanosecond  => DateTimeType<DateOnly>.DateTime64Nanosecond,
            };
            writeExpression = Expression.Call(Expression.Constant(dateTimeType), DateTimeType<DateOnly>.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType == typeof(IPAddress))
        {
            writeExpression = Expression.Call(Expression.Constant(IPType.Instance), IPType.WriteMethod, binaryWriterParameter, getExpression);
        }
        else if (elementType.IsGenericType && elementType.GetInterfaces().Any(x => x == typeof(IDictionary)))
        {
            var keyType = elementType.GetGenericArguments()[0];
            var valueType = elementType.GetGenericArguments()[1];
            var writeMethod = typeof(MapType).GetMethod(nameof(MapType.WriteCount), BindingFlags.Public | BindingFlags.Static)!.MakeGenericMethod(keyType, valueType);
            writeExpression = Expression.Call(writeMethod, binaryWriterParameter, getExpression);
            Statement(writeExpression);
            ForEach(getExpression, row =>
            {
                SetWriteExpression(keyType, binaryWriterParameter, Expression.Property(row, "Key"));
                SetWriteExpression(valueType, binaryWriterParameter, Expression.Property(row, "Value"));
            });
            return;
        }
        else if (elementType.IsGenericType && elementType.GetInterfaces().Any(x => x == typeof(IEnumerable)))
        {
            var genericType = elementType.GetGenericArguments()[0];
            var writeMethod = typeof(EnumerableType).GetMethod(nameof(EnumerableType.WriteCount), BindingFlags.Public | BindingFlags.Static)!.MakeGenericMethod(genericType);
            writeExpression = Expression.Call(writeMethod, binaryWriterParameter, getExpression);
            Statement(writeExpression);
            ForEach(getExpression, row =>
            {
                SetWriteExpression(genericType, binaryWriterParameter, row);
            });
            return;
        }
        else if (elementType.GetInterfaces().Any(x => x == typeof(ITuple)))
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
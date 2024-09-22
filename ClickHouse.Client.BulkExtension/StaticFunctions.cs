using System.Collections;
using System.Linq.Expressions;
using System.Net;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using ClickHouse.Client.BulkExtension.Annotation;
using ClickHouse.Client.BulkExtension.Numerics;
using ClickHouse.Client.BulkExtension.Types;
using DotNext.Linq.Expressions;
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

    public static void SetWriteExpression(Type elementType, Expression writerParameter, MemberExpression getExpression)
    {
        Expression writeExpression;

        if (elementType.IsValueType && elementType == typeof(Nullable<>).MakeGenericType(elementType))
        {
            var writeMethod = typeof(NullableType)
                .GetMethod(nameof(NullableType.WriteFlag), BindingFlags.Public | BindingFlags.Static)!.MakeGenericMethod(elementType)
                .CreateDelegate(typeof(Func<,,>).MakeGenericType(typeof(Memory<byte>), typeof(Nullable<>).MakeGenericType(elementType), typeof(int)));

            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(Nullable<>).MakeGenericType(elementType)),
                    Expression.Constant(writeMethod),
                    getExpression
                )
                .Await();

            Statement(writeExpression);
        }

        if (elementType == typeof(Guid))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(Guid)),
                    Expression.Constant(UuidType.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(bool))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(bool)),
                    Expression.Constant(BooleanType.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(byte))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(byte)),
                    Expression.Constant(UInt8Type.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(short))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(short)),
                    Expression.Constant(Int16Type.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(int))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(int)),
                    Expression.Constant(Int32Type.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(long))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(long)),
                    Expression.Constant(Int64Type.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(sbyte))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(sbyte)),
                    Expression.Constant(Int8Type.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(ushort))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(ushort)),
                    Expression.Constant(UInt16Type.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(uint))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(uint)),
                    Expression.Constant(UInt32Type.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(ulong))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(ulong)),
                    Expression.Constant(UInt64Type.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(float))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(float)),
                    Expression.Constant(Float32Type.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(double))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(double)),
                    Expression.Constant(Float64Type.Instance.Write),
                    getExpression
                )
                .Await();
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
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(BigInteger)),
                    Expression.Constant(instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(decimal))
        {
            var columnAttribute = getExpression.Member.GetCustomAttribute<ClickHouseColumnAttribute>();
            var precision = columnAttribute?.Precision ?? 16;
            var scale = columnAttribute?.Scale ?? 4;
            var instance = new DecimalType(precision, scale);
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(decimal)),
                    Expression.Constant(new Func<Memory<byte>, decimal, int>(instance.Write)),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(ClickHouseDecimal))
        {
            var columnAttribute = getExpression.Member.GetCustomAttribute<ClickHouseColumnAttribute>();
            var precision = columnAttribute?.Precision ?? 16;
            var scale = columnAttribute?.Scale ?? 4;
            var instance = new DecimalType(precision, scale);
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(ClickHouseDecimal)),
                    Expression.Constant(new Func<Memory<byte>, ClickHouseDecimal, int>(instance.Write)),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(string))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.StringWriteMethod,
                    getExpression
                )
                .Await();
        }
        else if (elementType.IsEnum)
        {
            var size = Enum.GetValues(elementType).Length;
            Expression convertExpression;
            switch (size)
            {
                case <= byte.MaxValue:
                    convertExpression = Expression.Convert(getExpression, typeof(byte));
                    writeExpression = Expression.Call
                        (
                            writerParameter,
                            ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(byte)),
                            Expression.Constant(UInt8Type.Instance.Write),
                            convertExpression
                        )
                        .Await();
                    break;
                case <= short.MaxValue:
                    convertExpression = Expression.Convert(getExpression, typeof(short));
                    writeExpression = Expression.Call
                        (
                            writerParameter,
                            ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(short)),
                            Expression.Constant(UInt16Type.Instance.Write),
                            convertExpression
                        )
                        .Await();
                    break;
                default:
                    convertExpression = Expression.Convert(getExpression, typeof(int));
                    writeExpression = Expression.Call
                        (
                            writerParameter,
                            ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(int)),
                            Expression.Constant(UInt32Type.Instance.Write),
                            convertExpression
                        )
                        .Await();
                    break;
            }
        }
        else if (elementType == typeof(DateTime))
        {
            var precision = getExpression.Member.GetCustomAttribute<ClickHouseColumnAttribute>()?.DateTimePrecision ?? DateTimePrecision.Second;
            var instance = precision switch
            {
                DateTimePrecision.Second => DateTimeType<DateTime>.DateTime64Second,
                DateTimePrecision.Millisecond => DateTimeType<DateTime>.DateTime64Millisecond,
                DateTimePrecision.Microsecond => DateTimeType<DateTime>.DateTime64Microsecond,
                DateTimePrecision.Nanosecond => DateTimeType<DateTime>.DateTime64Nanosecond,
            };
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(DateTime)),
                    Expression.Constant(instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(DateTimeOffset))
        {
            var precision = getExpression.Member.GetCustomAttribute<ClickHouseColumnAttribute>()?.DateTimePrecision ?? DateTimePrecision.Second;
            var instance = precision switch
            {
                DateTimePrecision.Second      => DateTimeType<DateTimeOffset>.DateTime64Second,
                DateTimePrecision.Millisecond => DateTimeType<DateTimeOffset>.DateTime64Millisecond,
                DateTimePrecision.Microsecond => DateTimeType<DateTimeOffset>.DateTime64Microsecond,
                DateTimePrecision.Nanosecond  => DateTimeType<DateTimeOffset>.DateTime64Nanosecond,
            };
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(DateTimeOffset)),
                    Expression.Constant(instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(DateOnly))
        {
            var precision = getExpression.Member.GetCustomAttribute<ClickHouseColumnAttribute>()?.DateTimePrecision ?? DateTimePrecision.Second;
            var instance = precision switch
            {
                DateTimePrecision.Second      => DateTimeType<DateOnly>.DateTime64Second,
                DateTimePrecision.Millisecond => DateTimeType<DateOnly>.DateTime64Millisecond,
                DateTimePrecision.Microsecond => DateTimeType<DateOnly>.DateTime64Microsecond,
                DateTimePrecision.Nanosecond  => DateTimeType<DateOnly>.DateTime64Nanosecond,
            };
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(DateOnly)),
                    Expression.Constant(instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType == typeof(IPAddress))
        {
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(IPAddress)),
                    Expression.Constant(IPType.Instance.Write),
                    getExpression
                )
                .Await();
        }
        else if (elementType.IsGenericType && elementType.GetInterfaces().Any(x => x == typeof(IDictionary)))
        {
            var keyType = elementType.GetGenericArguments()[0];
            var valueType = elementType.GetGenericArguments()[1];
            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(IDictionary)),
                    Expression.Constant(MapType.Instance.WriteCount),
                    getExpression
                )
                .Await();
            Statement(writeExpression);
            ForEach(getExpression, row =>
            {
                SetWriteExpression(keyType, writerParameter, Expression.Property(row, "Key"));
                SetWriteExpression(valueType, writerParameter, Expression.Property(row, "Value"));
            });
            return;
        }
        else if (elementType.IsGenericType && elementType.GetInterfaces().Any(x => x == typeof(IEnumerable)))
        {
            var genericType = elementType.GetGenericArguments()[0];
            var writeMethod = typeof(EnumerableType<>)
                .MakeGenericType(genericType)
                .GetMethod(nameof(EnumerableType<object>.WriteCount), BindingFlags.Public | BindingFlags.Static)!
                .CreateDelegate
                (
                    typeof(Func<,,>).MakeGenericType
                    (
                        typeof(Memory<byte>),
                        typeof(IEnumerable<>).MakeGenericType(genericType),
                        typeof(int)
                    )
                );

            writeExpression = Expression.Call
                (
                    writerParameter,
                    ClickHouseWriter.WriteMethod.MakeGenericMethod(typeof(IEnumerable<>).MakeGenericType(genericType)),
                    Expression.Constant(writeMethod),
                    getExpression
                )
                .Await();
            Statement(writeExpression);
            ForEach(getExpression, row =>
            {
                SetWriteExpression(genericType, writerParameter, row);
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
                SetWriteExpression(genericTypes[i], writerParameter, memberExpression);
            }
            return;
        }
        else
        {
            throw new NotSupportedException($"Type {elementType} is not supported");
        }

        Statement(writeExpression);
    }

    public static int Write7BitEncodedInt(this Memory<byte> buffer, int value)
    {
        var uValue = (uint)value;

        // Write out an int 7 bits at a time. The high bit of the byte,
        // when on, tells reader to continue reading more bytes.
        //
        // Using the constants 0x7F and ~0x7F below offers smaller
        // codegen than using the constant 0x80.

        var written = 0;
        while (uValue > 0x7Fu)
        {
            written += UInt8Type.Instance.Write(buffer[written..], (byte)(uValue | ~0x7Fu));
            uValue >>= 7;
        }

        written += UInt8Type.Instance.Write(buffer[written..], (byte)uValue);
        return written;
    }
}
using System.Reflection;
using System.Runtime.CompilerServices;

namespace ClickHouse.BulkExtension.Types;

class ByteArray
{
    public static readonly MethodInfo AsMemoryMethod = typeof(MemoryExtensions)
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .SingleOrDefault(x =>
            x.Name == nameof(MemoryExtensions.AsMemory)
            && x.IsGenericMethod
            && x.GetParameters().Length == 1
            && x.GetParameters()[0].ParameterType.BaseType == typeof(Array)
        )!
        .MakeGenericMethod(typeof(byte));

    public static readonly ByteArray Instance = new ByteArray();

    private ByteArray() { }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Write(Memory<byte> buffer, Memory<byte> value)
    {
        var span = buffer.Span;
        int written;
        if (value.Length < 255)
        {
            span[0] = (byte)value.Length; // bypass call to Write7BitEncodedInt
            written = 1;
        }
        else
        {
            written = buffer.Write7BitEncodedInt(value.Length);
        }
        value.Span.CopyTo(span);
        return value.Length + written;
    }
}
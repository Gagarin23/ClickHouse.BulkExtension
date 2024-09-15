using System.Runtime.CompilerServices;

namespace ClickHouse.Client.BulkExtension.Types.Base;

abstract class TupleType<T>
{
    public void Write(BinaryWriter writer, ITuple tuple)
    {
        for (var i = 0; i < tuple.Length; i++)
        {
            Write(writer, (T)tuple[i]);
        }
    }

    public abstract void Write(BinaryWriter writer, T value);
}
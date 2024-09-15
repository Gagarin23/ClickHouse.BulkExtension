using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

public class UuidType
{
    public static readonly MethodInfo WriteMethod = typeof(UuidType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly UuidType Instance = new UuidType();

    private UuidType() { }

    public void Write(BinaryWriter writer, Guid value)
    {
        Span<byte> guid = stackalloc byte[16];
        Span<byte> destination = stackalloc byte[16];
        value.TryWriteBytes(guid);

        guid[8..].Reverse();
        guid[6..8].CopyTo(destination[..2]);
        guid[4..6].CopyTo(destination[2..4]);
        guid[..4].CopyTo(destination[4..8]);
        guid[8..].CopyTo(destination[8..]);

        writer.Write(destination);
    }
}
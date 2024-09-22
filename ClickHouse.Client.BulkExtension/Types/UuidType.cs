namespace ClickHouse.Client.BulkExtension.Types;

public class UuidType
{
    public static readonly UuidType Instance = new UuidType();

    private UuidType() { }

    public int Write(Memory<byte> buffer, Guid value)
    {
        Span<byte> guid = stackalloc byte[16];
        var local = buffer.Span[..16];
        value.TryWriteBytes(guid);

        guid[8..].Reverse();
        guid[6..8].CopyTo(local[..2]);
        guid[4..6].CopyTo(local[2..4]);
        guid[..4].CopyTo(local[4..8]);
        guid[8..].CopyTo(local[8..]);

        return 16;
    }
}
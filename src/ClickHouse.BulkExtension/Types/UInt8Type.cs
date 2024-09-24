namespace ClickHouse.BulkExtension.Types;

class UInt8Type
{
    public static readonly UInt8Type Instance = new UInt8Type();

    private UInt8Type() { }

    public int Write(Memory<byte> buffer, byte value)
    {
        buffer.Span[0] = value;
        return sizeof(byte);
    }
}
using System.Net;
using System.Net.Sockets;
using System.Reflection;

namespace ClickHouse.Client.BulkExtension.Types;

class IPType
{
    public static readonly MethodInfo WriteMethod = typeof(IPType).GetMethod(nameof(Write), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly IPType Instance = new IPType();

    private IPType() { }

    public void Write(BinaryWriter writer, IPAddress value)
    {
        if (value.AddressFamily == AddressFamily.InterNetwork)
        {
            Span<byte> buffer = stackalloc byte[4];
            value.TryWriteBytes(buffer, out _);
            buffer.Reverse();
            writer.Write(buffer);
        }
        else if (value.AddressFamily != AddressFamily.InterNetworkV6)
        {
            Span<byte> buffer = stackalloc byte[16];
            value.TryWriteBytes(buffer, out _);
            buffer.Reverse();
            writer.Write(buffer);
        }
        else
        {
            throw new ArgumentException("Only IPv4 and IPv6 addresses are supported", nameof(value));
        }
    }
}
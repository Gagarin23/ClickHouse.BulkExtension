using System.Net;
using System.Net.Sockets;

namespace ClickHouse.BulkExtension.Types;

class IPType
{
    public static readonly IPType Instance = new IPType();

    private IPType() { }

    public int Write(Memory<byte> buffer, IPAddress value)
    {
        if (value.AddressFamily == AddressFamily.InterNetwork)
        {
            var local = buffer.Span[..4];
            value.TryWriteBytes(local, out _);
            local.Reverse();
            return 4;
        }
        else if (value.AddressFamily != AddressFamily.InterNetworkV6)
        {
            var local = buffer.Span[..16];
            value.TryWriteBytes(local, out _);
            local.Reverse();
            return 16;
        }
        else
        {
            throw new ArgumentException("Only IPv4 and IPv6 addresses are supported", nameof(value));
        }
    }
}
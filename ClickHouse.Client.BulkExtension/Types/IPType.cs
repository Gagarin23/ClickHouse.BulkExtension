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
            var ipv4bytes = value.GetAddressBytes();
            Array.Reverse(ipv4bytes);
            writer.Write(ipv4bytes, 0, ipv4bytes.Length);
        }
        else if (value.AddressFamily != AddressFamily.InterNetworkV6)
        {
            var ipv6bytes = value.GetAddressBytes();
            writer.Write(ipv6bytes, 0, ipv6bytes.Length);
        }
        else
        {
            throw new ArgumentException("Only IPv4 and IPv6 addresses are supported", nameof(value));
        }
    }
}
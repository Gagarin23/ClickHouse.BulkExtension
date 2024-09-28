using System.Threading.Channels;
using Microsoft.Extensions.Options;

namespace ClickHouse.Client.BulkExtension.WebApiAggregator;

public class ChannelHolder
{
    public ChannelWriter<YourStructType> Writer { get; }
    public ChannelReader<YourStructType> Reader { get; }

    public ChannelHolder(IOptions<AppOptions> options)
    {
        var channel = Channel.CreateBounded<YourStructType>(new BoundedChannelOptions(options.Value.ChannelCapacity)
        {
            SingleWriter = false,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        Reader = channel.Reader;
        Writer = channel.Writer;
    }
}
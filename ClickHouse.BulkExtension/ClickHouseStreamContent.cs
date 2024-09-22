using System.Net;

namespace ClickHouse.BulkExtension;

public class ClickHouseStreamContent : HttpContent
{
    private readonly Func<Stream, CancellationToken, Task> _onStreamAvailable;

    public ClickHouseStreamContent(Func<Stream, CancellationToken, Task> onStreamAvailable)
    {
        _onStreamAvailable = onStreamAvailable ?? throw new ArgumentNullException(nameof(onStreamAvailable));
    }

    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
    {
        return _onStreamAvailable(stream, default);
    }

    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
    {
        return _onStreamAvailable(stream, cancellationToken);
    }

    protected override bool TryComputeLength(out long length)
    {
        length = -1; // Длина неизвестна
        return false;
    }
}
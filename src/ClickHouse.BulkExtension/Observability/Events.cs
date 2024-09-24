using System.Diagnostics.Tracing;

namespace ClickHouse.BulkExtension.Observability;

[EventSource(Name = EventSourceName, Guid = "{8C9D2A55-EAE6-4760-86BF-5247EB9516C1}")]
public class Events : EventSource
{
    public const string EventSourceName = "ClickHouse.BulkExtension";
    public static readonly Events Writer = new Events();

    [Event(1, Level = EventLevel.Error)]
    public void Error(string source, Exception e)
    {
        WriteEvent(1, source, e.ToString());
    }
}
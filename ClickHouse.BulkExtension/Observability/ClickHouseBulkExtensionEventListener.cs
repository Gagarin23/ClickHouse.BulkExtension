using System.Diagnostics.Tracing;

namespace ClickHouse.BulkExtension.Observability;

public class ClickHouseBulkExtensionEventListener : EventListener
{
    private readonly EventLevel _eventLevel;
    private Action<EventWrittenEventArgs> _onEvent;

    public ClickHouseBulkExtensionEventListener(EventLevel eventLevel, Action<EventWrittenEventArgs> onEvent)
    {
        _eventLevel = eventLevel;
        _onEvent = onEvent;
    }

    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if (eventSource.Name == Events.EventSourceName)
        {
            EnableEvents(eventSource, _eventLevel, EventKeywords.All);
        }
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        if (eventData.EventSource.Name == Events.EventSourceName)
        {
            _onEvent(eventData);
        }
    }

    public override void Dispose()
    {
        _onEvent = null!;
        base.Dispose();
    }
}
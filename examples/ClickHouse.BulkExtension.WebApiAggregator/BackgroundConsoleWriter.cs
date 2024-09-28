using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ClickHouse.Client.BulkExtension.WebApiAggregator;

public class BackgroundConsoleWriter : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == nameof(BackgroundCopy))
            {
                l.EnableMeasurementEvents(instrument);
            }
        };

        var watcher = new Stopwatch();

        listener.SetMeasurementEventCallback<int>((_, measurement, _, _) =>
        {
            Console.Clear();
            Console.WriteLine($"Rows inserted: {measurement}");
            Console.WriteLine($"Rows per second: {Math.Round(measurement / watcher.Elapsed.TotalSeconds, 2)}");
        });

        listener.Start();

        while (true)
        {
            watcher.Start();
            listener.RecordObservableInstruments();
            await Task.Delay(1000, stoppingToken);
        }
    }
}
namespace ClickHouse.Client.BulkExtension.WebApiAggregator;

public class AppOptions
{
    public string ConnectionString { get; set; }
    public int ChannelCapacity { get; set; }
    public int MaxBatchSize { get; set; }
    public int MaxBatchDurationInSeconds { get; set; }
}
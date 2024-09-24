namespace ClickHouse.Client.BulkExtension.MinimalWebApi;

public class AppOptions
{
    public string ConnectionString { get; set; }
    public int ChannelCapacity { get; set; }
    public int MaxBatchSize { get; set; }
    public int MaxBatchDurationInSeconds { get; set; }
}
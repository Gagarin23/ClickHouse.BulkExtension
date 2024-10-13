using System.Diagnostics;
using System.Threading.Channels;
using ClickHouse.BulkExtension;
using ClickHouse.BulkExtension.Annotation;
using ClickHouse.BulkExtension.Profiling;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;

if(args[0] == "Extension")
{
    await BulkExtensionProfiling.RunAsync();
}
else if(args[0] == "Client")
{
    await ChClientProfiling.RunAsync();
}
else
{
    Console.WriteLine("Unknown argument");
}
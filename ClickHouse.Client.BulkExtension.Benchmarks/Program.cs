using BenchmarkDotNet.Running;
using ClickHouse.Client.BulkExtension.Benchmarks;

//BenchmarkRunner.Run<BulkInsertBench>();
const int l = 1;
var bench = new BulkInsertBench();
var tasks = new List<Task>(l);
await bench.GlobalSetup();
for (int i = 0; i < l; i++)
{
    //Console.WriteLine("start");
//await bench.BulkInsertInt32();
    //await bench.NewBulkInsertInt32();
    //await bench.BulkInsertEntity();
    //await bench.NewBulkInsertEntity();
    //await bench.NewAsyncBulkInsertEntity();
    //await Task.Delay(500);
    tasks.Add(Task.Run(() => bench.NewBulkInsertEntity()));
    //tasks.Add(Task.Run(() => bench.BulkInsertEntity()));
}

await Task.WhenAll(tasks);



using BenchmarkDotNet.Running;
using ClickHouse.BulkExtension.Benchmarks;

BenchmarkRunner.Run<BulkInsertBench>();
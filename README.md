# ClickHouse.Client.BulkExtension

## Overview
ClickHouseBulkExtension is a high-performance .NET library designed for efficient bulk insertion of data into ClickHouse databases via HTTP streaming. It supports both synchronous and asynchronous data sources, including non-materialized collections, and provides advanced features like dynamic code compilation and optional data compression.

## Features
- Support for IEnumerable and IAsyncEnumerable: Stream data directly from synchronous or asynchronous enumerables without materializing the entire collection in memory.
- Dynamic Code Compilation: Utilizes dynamic code generation to avoid boxing of value types, improving performance and reducing memory allocations.
- Optional Compression: Supports optional data compression (e.g., GZip) to reduce network bandwidth usage.
- High Performance: Designed for high-throughput scenarios, optimized to minimize CPU usage and memory allocations.
- Minimal Memory Allocations: Efficiently handles large datasets with minimal impact on memory consumption.
- Easy Integration: Seamlessly integrates with existing projects using ClickHouse.

## Benchmark Results
Below are benchmark results comparing different methods of bulk insertion, highlighting the performance benefits of ClickHouseBulkExtension.

### Environment:
- Operating System: Windows 11 (10.0.22631.4169/23H2/2023Update/SunValley3)
- Processor: AMD Ryzen 7 5800X, 1 CPU, 16 logical and 8 physical cores
- .NET SDK: 8.0.108

### BenchmarkDotNet v0.14.0

| Method                                         | Count     | Mean         | Allocated     |
|------------------------------------------------|-----------|--------------|---------------|
| ClickHouse.Client.Copy_ComplexStruct           | 10,000    | 31.05 ms     | 7,211.42 KB   |
| ClickHouse.Client.BulkExtension_ComplexStruct  | 10,000    | 60.53 ms     | 10.85 KB      |
| ClickHouse.Client.Copy_ComplexStruct           | 100,000   | 195.88 ms    | 69,989.35 KB  |
| ClickHouse.Client.BulkExtension_ComplexStruct  | 100,000   | 199.56 ms    | 11.25 KB      |
| ClickHouse.Client.Copy_ComplexStruct           | 300,000   | 582.16 ms    | 210,602.02 KB |
| ClickHouse.Client.BulkExtension_ComplexStruct  | 300,000   | 518.25 ms    | 12.77 KB      |
| ClickHouse.Client.Copy_ComplexStruct           | 1,000,000 | 2,007.25 ms  | 696,371.72 KB |
| ClickHouse.Client.BulkExtension_ComplexStruct  | 1,000,000 | 1,599.89 ms  | 12.23 KB      |

### Interpretation of Results:
- Memory Allocations: The methods using ClickHouseBulkExtension (BulkExtension_ComplexStruct) show significantly lower memory allocations compared to traditional bulk insert methods. This indicates efficient memory usage, especially important when handling large datasets.
- Performance: Execution times are competitive, and in some cases, the new methods outperform the traditional ones, demonstrating the high performance of the library.
- Scalability: The library maintains consistent performance and low memory usage even as the data volume increases to 1,000,000 records.

## Resource consumption
The library is designed to minimize resource consumption. By efficiently streaming data to ClickHouse, it reduces the impact on system resources and improves overall performance.\
Let's take a look at the resource consumption of the library in one-minute work with one million records batch and total insertion of about 100 million records:

![](\examples\ClickHouse.BulkExtension.Profiling\memprof_1.png)
![](\examples\ClickHouse.BulkExtension.Profiling\memprof_2.png)

For profiling, a structure with three fields and a total size of 24 bytes was used. Given the total insertion of 100 million records, the payload traffic will be about 2.4 gigabytes. The screenshot shows the memory consumption of the library over the entire period of operation - 200.6 kilobytes, which is about 0.008% of the total payload traffic.\
The test console utility and the profiling file are located in the [ClickHouse.BulkExtension.Profiling](https://github.com/Gagarin23/ClickHouse.Client.BulkExtension/tree/main/examples/ClickHouse.BulkExtension.Profiling) folder.

## Getting Started

### Installation
The library is available as a NuGet package. You can install it using the following command:

```bash
dotnet add package ClickHouse.Client.BulkExtension
```

### Usage
The [examples](https://github.com/Gagarin23/ClickHouse.Client.BulkExtension/tree/main/examples) folder contains examples of using ClickHouse.Client.BulkExtension for both synchronous and asynchronous data insertion.

### Dynamic Code Compilation
ClickHouseBulkExtension uses dynamic code compilation to generate optimized serialization code at runtime. This avoids boxing of value types, reducing memory allocations and improving performance.

## How It Works
- Streaming Data: The library streams data directly to ClickHouse using HTTP streaming, supporting both synchronous (IEnumerable) and asynchronous (IAsyncEnumerable) data sources.
- Optimized Serialization: By dynamically generating serialization code, the library efficiently converts your data types into the binary format expected by ClickHouse.
- Compression: Optional compression reduces the amount of data transmitted over the network, which can improve performance when network bandwidth is a limiting factor.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

---

 Disclaimer: The benchmark results are based on specific hardware and software configurations. Your results may vary depending on your environment.

---

Feel free to explore the library and integrate it into your projects for efficient and high-performance data insertion into ClickHouse!
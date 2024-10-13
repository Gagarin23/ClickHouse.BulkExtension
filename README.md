# ClickHouse.BulkExtension

[![NuGet](https://img.shields.io/nuget/v/ClickHouse.BulkExtension.svg)](https://www.nuget.org/packages/ClickHouse.BulkExtension/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

- [Overview](#overview)
- [Features](#features)
- [Benchmark Results](#benchmark-results)
    - [Environment](#environment)
    - [BenchmarkDotNet v0.14.0](#benchmarkdotnet-v0140)
    - [Interpretation of Results](#interpretation-of-results)
- [Resource Consumption](#resource-consumption)
- [Getting Started](#getting-started)
    - [Installation](#installation)
    - [Usage](#usage)
- [How It Works](#how-it-works)
- [License](#license)
- [Contributing](#contributing)
- [Support](#support)

## Overview
ClickHouse.BulkExtension is a high-performance .NET library designed for efficient bulk insertion of data into ClickHouse databases via streaming. It supports both synchronous and asynchronous data sources, including streaming from non-materialized collections, and provides advanced features like dynamic code compilation and optional data compression.

## Features

- **Support for `IEnumerable` and `IAsyncEnumerable`**: Stream data directly from synchronous or asynchronous enumerables without materializing the entire collection in memory.
- **Dynamic Code Compilation**: Uses dynamic code generation to avoid boxing of value types, improving performance and reducing memory allocations.
- **Optional Compression**: Supports optional data compression (e.g., GZip) to reduce network bandwidth usage.
- **High Performance**: Designed for high-throughput scenarios, optimized to minimize CPU usage and memory allocations.
- **Minimal Memory Allocations**: Efficiently handles large datasets with minimal impact on memory consumption.
- **Easy Integration**: Seamlessly integrates with existing projects using ClickHouse.

## Benchmark Results
Below are benchmark results comparing different methods of bulk insertion, highlighting the performance benefits of ClickHouseBulkExtension.

### Environment:
- Operating System: Windows 11 (10.0.22631.4169/23H2/2023Update/SunValley3)
- Processor: AMD Ryzen 7 5800X, 1 CPU, 16 logical and 8 physical cores
- .NET SDK: 8.0.108

### BenchmarkDotNet v0.14.0

**Disclaimer**: The benchmark results are based on specific hardware and software configurations. Your results may vary depending on your environment.

| Method                                                  | Count     | Mean (ms)  | Allocated (KB) |
|---------------------------------------------------------|-----------|------------|----------------|
| **Traditional Bulk Insert**                             | 10,000    | 31.05      | 7,211.42       |
| **BulkExtension (Complex Struct)**                      | 10,000    | 60.53      | 10.85          |
| **BulkExtension (Complex Struct) without compression**  | 10,000    | 52.29      | 10.57          |
| **Traditional Bulk Insert**                             | 100,000   | 195.88     | 69,989.35      |
| **BulkExtension (Complex Struct)**                      | 100,000   | 199.56     | 11.25          |
| **BulkExtension (Complex Struct) without compression**  | 100,000   | 123.70     | 10.63          |
| **Traditional Bulk Insert**                             | 300,000   | 582.16     | 210,602.02     |
| **BulkExtension (Complex Struct)**                      | 300,000   | 518.25     | 12.77          |
| **BulkExtension (Complex Struct) without compression**  | 300,000   | 285.06     | 11.55          |
| **Traditional Bulk Insert**                             | 1,000,000 | 2,007.25   | 696,371.72     |
| **BulkExtension (Complex Struct)**                      | 1,000,000 | 1,599.89   | 12.23          |
| **BulkExtension (Complex Struct) without compression**  | 1,000,000 | 838.29     | 12.86          |

- Traditional Bulk Insert: Using ClickHouse.Client.Copy method for bulk insertion with complex structures.
- Traditional Bulk Insert without compression: No results, because ClickHouse.Client.Copy does not support compression switch off. (why?!)
- BulkExtension (Complex Struct): Using ClickHouse.Client.BulkExtension with complex structures.
- BulkExtension (Complex Struct) without compression: Using ClickHouse.Client.BulkExtension with complex structures and no compression. 

#### Interpretation of Results

- **Memory Allocations**: The methods using ClickHouse.Client.BulkExtension show **over 99% reduction** in memory allocations compared to traditional methods. This demonstrates efficient memory usage, crucial for large datasets.
- **Performance**: Execution times are competitive. In cases with 1,000,000 records, `BulkExtension` performs faster than traditional methods, showcasing its high performance.
- **Scalability**: The library maintains consistent performance and low memory usage even as data volume increases, proving its scalability.

## Resource Consumption

The library is designed to minimize resource consumption by efficiently streaming data to ClickHouse, reducing the impact on system resources, and improving overall performance.

Let's examine the resource consumption during a one-minute operation with a batch of one million records, totaling approximately 100 million records inserted:

*Figure 1: Comparison of memory consumption over the operation period.*
*BulkExtension:*
![Memory Profile 1](https://github.com/Gagarin23/ClickHouse.Client.BulkExtension/blob/main/examples/ClickHouse.BulkExtension.Profiling/memprof_1.png)
*ClickHouse.Client.Copy:*
![Memory Profile 4](https://github.com/Gagarin23/ClickHouse.Client.BulkExtension/blob/main/examples/ClickHouse.BulkExtension.Profiling/memprof_4.png)
*Figure 2: Comparison of detailed namespace memory allocation analysis.*
*BulkExtension:*
![Memory Profile 2](https://github.com/Gagarin23/ClickHouse.Client.BulkExtension/blob/main/examples/ClickHouse.BulkExtension.Profiling/memprof_2.png)
*ClickHouse.Client.Copy:*
![Memory Profile 5](https://github.com/Gagarin23/ClickHouse.Client.BulkExtension/blob/main/examples/ClickHouse.BulkExtension.Profiling/memprof_5.png)
*Figure 3: Comparison of detailed assembly memory allocation analysis.*
*BulkExtension:*
![Memory Profile 3](https://github.com/Gagarin23/ClickHouse.Client.BulkExtension/blob/main/examples/ClickHouse.BulkExtension.Profiling/memprof_3.png)
*ClickHouse.Client.Copy:*
![Memory Profile 6](https://github.com/Gagarin23/ClickHouse.Client.BulkExtension/blob/main/examples/ClickHouse.BulkExtension.Profiling/memprof_6.png)

For profiling, a structure with three fields and a total size of 24 bytes was used. Given the total insertion of 100 million records, the payload traffic amounts to approximately 2.4 gigabytes. The screenshots show the library's memory consumption over the entire period of operation—216 kilobytes—which is about 0.008% of the total payload traffic.

The test console utility and the profiling file are located in the [ClickHouse.BulkExtension.Profiling](https://github.com/Gagarin23/ClickHouse.Client.BulkExtension/tree/main/examples/ClickHouse.BulkExtension.Profiling) folder (`ClickHouse.BulkExtension.Profiling.dmw` for dotMemory).

## Getting Started

### Installation

You can install the library via NuGet:

- To install the full package with the client:

```bash
dotnet add package ClickHouse.Client.BulkExtension
```
- If you only need the bulk copy functionality:
```bash
dotnet add package ClickHouse.BulkExtension
```
If you need just the bulk copy library.

### Usage
The [examples](https://github.com/Gagarin23/ClickHouse.Client.BulkExtension/tree/main/examples) folder contains examples of using ClickHouse.Client.BulkExtension for both synchronous and asynchronous data insertion.

### Dynamic Code Compilation
ClickHouseBulkExtension uses dynamic code compilation to generate optimized serialization code at runtime. This avoids boxing of value types, reducing memory allocations and improving performance.

## How It Works
- Streaming Data: The library streams data directly to ClickHouse using HTTP streaming, supporting both synchronous (IEnumerable) and asynchronous (IAsyncEnumerable) data sources.
- Optimized Serialization: By dynamically generating serialization code, the library efficiently converts your data types into the binary format expected by ClickHouse.
- Compression: Optional compression reduces the amount of data transmitted over the network, which can improve performance when network bandwidth is a limiting factor.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! If you find a bug or have a feature request, please open an issue on the [GitHub repository](https://github.com/Gagarin23/ClickHouse.Client.BulkExtension). Pull requests are also appreciated.

## Support

For questions or assistance, feel free to open an issue or contact us at [cuzitworks92@gmail.com](mailto:cuzitworks92@gmail.com).

---

Feel free to explore the library and integrate it into your projects for efficient and high-performance data insertion into ClickHouse. We look forward to your feedback and contributions!


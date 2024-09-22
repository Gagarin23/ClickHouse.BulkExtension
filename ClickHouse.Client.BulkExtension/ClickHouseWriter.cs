using System.Buffers;
using System.Reflection;
using ClickHouse.Client.BulkExtension.Types;

namespace ClickHouse.Client.BulkExtension;

class ClickHouseWriter : IAsyncDisposable
{
    private const int BufferThreshold = 64;

    public static readonly MethodInfo WriteMethod = typeof(ClickHouseWriter).GetMethod(nameof(WriteAsync), BindingFlags.Public | BindingFlags.Instance)!;
    public static readonly MethodInfo StringWriteMethod = typeof(ClickHouseWriter).GetMethod(nameof(WriteStringAsync), BindingFlags.Public | BindingFlags.Instance)!;

    private readonly Stream _underlyingStream;
    private IMemoryOwner<byte> _memoryOwner;
    private int _position;

    public ClickHouseWriter(Stream underlyingStream, int bufferSize)
    {
        _underlyingStream = underlyingStream;
        _memoryOwner = MemoryPool<byte>.Shared.Rent(bufferSize);
    }

    public async ValueTask WriteStringAsync(string value)
    {
        var required = value.Length * 3;
        var bufferLength = _memoryOwner.Memory.Length;
        if (required >= bufferLength - _position)
        {
            await FlushAsync();
            Resize(required + bufferLength);
        }

        var bytesWritten = StringType.Instance.Write(_memoryOwner.Memory[_position..], value);
        _position += bytesWritten;
    }

    public async ValueTask WriteAsync<T>(Func<Memory<byte>, T, int> writeFunction, T value)
    {
        var memory = _memoryOwner.Memory;
        if (memory.Length - _position <= BufferThreshold)
        {
            await FlushAsync();
        }

        var written = writeFunction(memory[_position..], value);
        _position += written;
    }

    public async ValueTask DisposeAsync()
    {
        await FlushAsync();
        _memoryOwner.Dispose();
    }

    private async Task FlushAsync()
    {
        if (_position == 0)
        {
            return;
        }
        await _underlyingStream.WriteAsync(_memoryOwner.Memory[.._position]);
        _position = 0;
    }

    private void Resize(int required)
    {
        _memoryOwner.Dispose();
        _memoryOwner = MemoryPool<byte>.Shared.Rent(required);
        _position = 0;
    }
}
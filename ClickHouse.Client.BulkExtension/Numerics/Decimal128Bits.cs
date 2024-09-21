using System.Runtime.InteropServices;

namespace ClickHouse.Client.BulkExtension.Numerics;

[StructLayout(LayoutKind.Sequential)]
struct Decimal128Bits
{
    public ulong Low64;
    public ulong High64;

    public Decimal128Bits(decimal value)
    {
        // Преобразуем decimal в 128-битное число без выделения памяти
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);

        // Собираем 96-битное значение из частей
        ulong low = (uint)bits[0];
        ulong mid = (uint)bits[1];
        ulong high = (uint)bits[2];

        Low64 = low | (mid << 32);
        High64 = high;

        // Обрабатываем знак
        bool isNegative = (bits[3] & 0x80000000) != 0;
        if (isNegative)
        {
            // Для отрицательных чисел применяем дополнительный код
            AdditiveInverse(ref Low64, ref High64);
        }
    }

    private void AdditiveInverse(ref ulong low, ref ulong high)
    {
        low = ~low;
        high = ~high;

        low += 1;
        if (low == 0)
        {
            high += 1;
        }
    }
}
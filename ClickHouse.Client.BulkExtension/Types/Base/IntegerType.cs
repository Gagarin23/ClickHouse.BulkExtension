namespace ClickHouse.Client.BulkExtension.Types.Base;

abstract class IntegerType
{
    protected virtual bool Signed => true;
}
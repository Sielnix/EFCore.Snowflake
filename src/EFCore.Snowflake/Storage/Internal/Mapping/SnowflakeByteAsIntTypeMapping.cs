using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

internal class SnowflakeByteAsIntTypeMapping : ByteTypeMapping
{
    public new static SnowflakeByteAsIntTypeMapping Default { get; } = new();

    public SnowflakeByteAsIntTypeMapping(string? storeType = null)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(byte),
                    converter: new ValueConverterImpl()
                    ),
                storeType: storeType ?? SnowflakeStoreTypeNames.GetIntegerTypeToHoldEverything(
                    SignedIntegerType.Byte),
                dbType: System.Data.DbType.Int64))
    {
    }

    protected SnowflakeByteAsIntTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeByteAsIntTypeMapping(parameters);
    }

    private sealed class ValueConverterImpl() : ValueConverter<byte, long>(b => (long)b, l => (byte)l);
}

using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeByteAsIntTypeMapping : ByteTypeMapping
{
    public new static SnowflakeByteAsIntTypeMapping Default { get; } = new();

    public SnowflakeByteAsIntTypeMapping(int? precision = null)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(byte),
                    converter: new ValueConverterImpl(),
                    jsonValueReaderWriter: JsonByteReaderWriter.Instance
                    ),
                storeType: SnowflakeStoreTypeNames.Number,
                precision: precision ?? SnowflakeStoreTypeNames.GetIntegerTypePrecisionToHoldEverything(SignedIntegerType.Byte),
                scale: SnowflakeStoreTypeNames.IntegerTypeScale,
                dbType: System.Data.DbType.Int64,
                storeTypePostfix: StoreTypePostfix.PrecisionAndScale))
    {
    }

    protected SnowflakeByteAsIntTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeByteAsIntTypeMapping(parameters);
    }

    private sealed class ValueConverterImpl() : ValueConverter<byte, long>(b => (long)b, l => (byte)l);
}

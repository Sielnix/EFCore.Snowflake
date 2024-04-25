using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeUShortTypeMapping : UShortTypeMapping
{
    public new static SnowflakeUShortTypeMapping Default { get; } = new();

    public SnowflakeUShortTypeMapping()
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(ushort),
                    converter: new ValueConverterImpl(),
                    jsonValueReaderWriter: JsonUInt16ReaderWriter.Instance
                ),
                storeType: "NUMBER(5,0)",
                dbType: System.Data.DbType.Int64,
                precision: 5,
                scale: 0,
                storeTypePostfix: StoreTypePostfix.PrecisionAndScale))
    {
    }

    protected SnowflakeUShortTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeUShortTypeMapping(parameters);
    }

    private sealed class ValueConverterImpl() : ValueConverter<ushort, long>(b => (long)b, l => (ushort)l);
}

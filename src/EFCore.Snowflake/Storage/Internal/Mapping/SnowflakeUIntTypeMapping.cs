using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeUIntTypeMapping : UIntTypeMapping
{
    public new static SnowflakeUIntTypeMapping Default { get; } = new();

    public SnowflakeUIntTypeMapping()
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(uint),
                    converter: new ValueConverterImpl(),
                    jsonValueReaderWriter: JsonUInt32ReaderWriter.Instance
                ),
                storeType: "NUMBER(10,0)",
                dbType: System.Data.DbType.Int64))
    {
    }

    protected SnowflakeUIntTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeUIntTypeMapping(parameters);
    }

    private sealed class ValueConverterImpl() : ValueConverter<uint, long>(b => (long)b, l => (uint)l);
}

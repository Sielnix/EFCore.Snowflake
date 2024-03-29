using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeSByteTypeMapping : SByteTypeMapping
{
    public new static SnowflakeSByteTypeMapping Default { get; } = new();

    public SnowflakeSByteTypeMapping()
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(sbyte),
                    converter: new ValueConverterImpl(),
                    jsonValueReaderWriter: JsonSByteReaderWriter.Instance
                ),
                storeType: "NUMBER(3,0)",
                dbType: System.Data.DbType.Int64))
    {
    }

    protected SnowflakeSByteTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeSByteTypeMapping(parameters);
    }

    private sealed class ValueConverterImpl() : ValueConverter<sbyte, long>(b => (long)b, l => (sbyte)l);
}

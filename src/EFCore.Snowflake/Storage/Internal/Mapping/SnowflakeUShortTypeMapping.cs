using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

internal class SnowflakeUShortTypeMapping : UShortTypeMapping
{
    public new static SnowflakeUShortTypeMapping Default { get; } = new();

    public SnowflakeUShortTypeMapping()
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(ushort),
                    converter: new ValueConverterImpl()
                ),
                storeType: "NUMBER(5, 0)",
                dbType: System.Data.DbType.Int64))
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

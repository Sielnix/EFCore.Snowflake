using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeULongTypeMapping : ULongTypeMapping
{
    public new static SnowflakeULongTypeMapping Default { get; } = new();

    public SnowflakeULongTypeMapping()
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(ulong),
                    converter: new ValueConverterImpl(),
                    jsonValueReaderWriter: JsonUInt64ReaderWriter.Instance
                ),
                storeType: "NUMBER(20,0)",
                dbType: System.Data.DbType.Int64))
    {
    }

    protected SnowflakeULongTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeULongTypeMapping(parameters);
    }

    private sealed class ValueConverterImpl() : ValueConverter<ulong, long>(b => (long)b, l => (ulong)l);
}

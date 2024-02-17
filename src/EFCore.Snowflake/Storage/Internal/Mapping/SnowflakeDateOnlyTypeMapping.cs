using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeDateOnlyTypeMapping : DateOnlyTypeMapping
{
    private const string DateOnlyFormatConst = @"'{0:yyyy\-MM\-dd}'";

    public new static SnowflakeDateOnlyTypeMapping Default { get; } = new();

    public SnowflakeDateOnlyTypeMapping()
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(DateOnly),
                    converter: new ValueConverterImpl()
                ),
                storeType: SnowflakeStoreTypeNames.Date,
                dbType: System.Data.DbType.DateTime))
    {
    }

    protected override string SqlLiteralFormatString => DateOnlyFormatConst;

    protected SnowflakeDateOnlyTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeDateOnlyTypeMapping(parameters);
    }

    private sealed class ValueConverterImpl() : ValueConverter<DateOnly, DateTime>(b => b.ToDateTime(default), dt => new DateOnly(dt.Year, dt.Month, dt.Day));
}

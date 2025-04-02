using System.Data.Common;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeDateTimeOffsetAsLtzTypeMapping : SnowflakeDateTimeOffsetTypeMapping
{
    public new static SnowflakeDateTimeOffsetAsLtzTypeMapping Default { get; } = new(SnowflakeStoreTypeNames.DefaultTimePrecision);

    public SnowflakeDateTimeOffsetAsLtzTypeMapping(int precision)
        : base(SnowflakeStoreTypeNames.GetTimeType(SnowflakeStoreTypeNames.TimestampLtz, precision), precision)
    {
    }

    protected SnowflakeDateTimeOffsetAsLtzTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeDateTimeOffsetAsLtzTypeMapping(parameters);
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        return SnowflakeDateTimeFormatter.GenerateNonNullSqlLiteral(value, StoreType, StoreTypeNameBase, Precision);
    }

    protected override void ConfigureParameter(DbParameter parameter)
    {
        object? value = parameter.Value;
        if (value is DateTimeOffset offset)
        {
            parameter.Value = offset.DateTime;
        }

        if (parameter.DbType == System.Data.DbType.DateTimeOffset)
        {
            parameter.DbType = System.Data.DbType.DateTime;
        }
    }
}

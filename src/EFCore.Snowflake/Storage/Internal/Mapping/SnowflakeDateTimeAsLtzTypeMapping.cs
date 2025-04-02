using System.Data.Common;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeDateTimeAsLtzTypeMapping : DateTimeTypeMapping
{
    public new static SnowflakeDateTimeAsLtzTypeMapping Default { get; } = new(SnowflakeStoreTypeNames.DefaultTimePrecision);

    public SnowflakeDateTimeAsLtzTypeMapping(int precision)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(DateTime),
                    converter: new ValueConverterImpl(),
                    jsonValueReaderWriter: JsonDateTimeReaderWriter.Instance
                ),
                storeType: SnowflakeStoreTypeNames.GetTimeType(SnowflakeStoreTypeNames.TimestampLtz, precision),
                dbType: System.Data.DbType.DateTimeOffset,
                precision: precision,
                storeTypePostfix: StoreTypePostfix.Precision))
    {
    }

    protected SnowflakeDateTimeAsLtzTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeDateTimeAsLtzTypeMapping(parameters);
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

    private sealed class ValueConverterImpl() : ValueConverter<DateTime, DateTimeOffset>(dt => new DateTimeOffset(dt), dto => dto.DateTime);
}

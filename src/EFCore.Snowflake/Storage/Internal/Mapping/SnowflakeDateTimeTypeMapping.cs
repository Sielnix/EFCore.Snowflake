using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeDateTimeTypeMapping : DateTimeTypeMapping
{
    public new static SnowflakeDateTimeTypeMapping Default { get; } = new(
        SnowflakeStoreTypeNames.GetTimeType(SnowflakeStoreTypeNames.TimestampNtz, SnowflakeStoreTypeNames.DefaultTimePrecision),
        SnowflakeStoreTypeNames.DefaultTimePrecision);

    public SnowflakeDateTimeTypeMapping(string storeTypeName, int? precision)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(DateTime),
                    jsonValueReaderWriter: JsonDateTimeReaderWriter.Instance
                ),
                storeType: storeTypeName,
                dbType: System.Data.DbType.DateTime,
                precision: precision,
                storeTypePostfix: StoreTypePostfix.Precision))
    {
    }

    protected SnowflakeDateTimeTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeDateTimeTypeMapping(parameters);
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        return SnowflakeDateTimeFormatter.GenerateNonNullSqlLiteral(value, StoreType, StoreTypeNameBase, Precision);
    }
}

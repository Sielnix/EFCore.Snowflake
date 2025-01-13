using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using System.Globalization;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeDateTimeOffsetTypeMapping : DateTimeOffsetTypeMapping
{
    public new static SnowflakeDateTimeOffsetTypeMapping Default { get; } = new(SnowflakeStoreTypeNames.TimestampTz, SnowflakeStoreTypeNames.DefaultTimePrecision);

    public SnowflakeDateTimeOffsetTypeMapping(string storeTypeName, int precision)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(DateTimeOffset),
                    jsonValueReaderWriter: JsonDateTimeOffsetReaderWriter.Instance
                ),
                storeType: SnowflakeStoreTypeNames.GetTimeType(storeTypeName, precision),
                dbType: System.Data.DbType.DateTimeOffset,
                precision: precision,
                storeTypePostfix: StoreTypePostfix.Precision))
    {
    }

    protected SnowflakeDateTimeOffsetTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeDateTimeOffsetTypeMapping(parameters);
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        DateTimeOffset offset = (DateTimeOffset)value;

        int precision = Precision ?? SnowflakeStoreTypeNames.DefaultTimePrecision;
        precision = Math.Min(precision, SnowflakeStoreTypeNames.MaxDotNetDateTimePrecision);

        string formatString =
            "yyyy-MM-dd HH:mm:ss."
            + (precision == 0 ? string.Empty : new string('f', precision))
            + "zzz";

        return "'" + offset.ToString(formatString, CultureInfo.InvariantCulture) + "'::" + StoreType;
    }
}

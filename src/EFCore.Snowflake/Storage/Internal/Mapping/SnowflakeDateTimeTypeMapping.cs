using System.Globalization;
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
                precision: precision))
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
        const string dateTimeFormat = "yyyy-MM-dd HH:mm:ss";
        const string timeFormat = "HH:mm:ss";
        const string dateFormat = "yyyy-MM-dd";

        string baseFormat = StoreTypeNameBase switch
        {
            SnowflakeStoreTypeNames.TimestampNtz => dateTimeFormat,
            SnowflakeStoreTypeNames.TimestampLtz => dateTimeFormat,
            SnowflakeStoreTypeNames.TimestampTz => dateTimeFormat,
            SnowflakeStoreTypeNames.Time => timeFormat,
            SnowflakeStoreTypeNames.Date => dateFormat,
            _ => throw new ArgumentOutOfRangeException($"Unsupported base type {StoreTypeNameBase}")
        };

        DateTime val = (DateTime)value;

        int? precision = Precision;

        string format = baseFormat;
        if (precision.HasValue && precision.Value > 0)
        {
            if (precision.Value > SnowflakeStoreTypeNames.MaxDotNetDateTimePrecision)
            {
                precision = SnowflakeStoreTypeNames.MaxDotNetDateTimePrecision;
            }

            // todo: optimize it
            format = format + "." + new string('f', precision.Value);
        }

        return "'" + val.ToString(format, CultureInfo.InvariantCulture) + "'::" + StoreType;
    }
}

using System.Globalization;

namespace EFCore.Snowflake.Storage.Internal.Mapping;
internal static class SnowflakeDateTimeFormatter
{
    public static string GenerateNonNullSqlLiteral(object value, string storeType, string storeTypeNameBase, int? precision)
    {
        const string dateTimeFormat = "yyyy-MM-dd HH:mm:ss";
        const string timeFormat = "HH:mm:ss";
        const string dateFormat = "yyyy-MM-dd";

        string baseFormat = storeTypeNameBase switch
        {
            SnowflakeStoreTypeNames.TimestampNtz => dateTimeFormat,
            SnowflakeStoreTypeNames.TimestampLtz => dateTimeFormat,
            SnowflakeStoreTypeNames.TimestampTz => dateTimeFormat,
            SnowflakeStoreTypeNames.Time => timeFormat,
            SnowflakeStoreTypeNames.Date => dateFormat,
            _ => throw new ArgumentOutOfRangeException($"Unsupported base type {storeTypeNameBase}")
        };

        DateTime val = (DateTime)value;

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

        return "'" + val.ToString(format, CultureInfo.InvariantCulture) + "'::" + storeType;
    }
}

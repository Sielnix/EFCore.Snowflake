using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeTimeOnlyTypeMapping : TimeOnlyTypeMapping
{
    public new static SnowflakeTimeOnlyTypeMapping Default { get; } = new(SnowflakeStoreTypeNames.DefaultTimePrecision);
    
    public SnowflakeTimeOnlyTypeMapping(int precision)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(TimeOnly),
                    converter: new ValueConverterImpl(),
                    comparer: new ValueComparerImpl(),
                    keyComparer: new ProviderValueComparerImpl(),
                    providerValueComparer: new ProviderValueComparerImpl(),
                    jsonValueReaderWriter: JsonTimeOnlyReaderWriter.Instance
                ),
                storeType: SnowflakeStoreTypeNames.GetTimeType(SnowflakeStoreTypeNames.Time, precision),
                dbType: System.Data.DbType.DateTime,
                size: precision))
    {
    }

    protected SnowflakeTimeOnlyTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeTimeOnlyTypeMapping(parameters);
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        DateTime dateTime = (DateTime)value;
        TimeOnly timeOnly = TimeOnly.FromDateTime(dateTime);

        return timeOnly.Ticks % TimeSpan.TicksPerSecond == 0
            ? FormattableString.Invariant($@"'{value:HH\:mm\:ss}'")
            : FormattableString.Invariant($@"'{value:HH\:mm\:ss\.fffffff}'");
    }

    private sealed class ValueConverterImpl() : ValueConverter<TimeOnly, DateTime>(b => new DateTime(2000, 1, 1).AddTicks(b.Ticks), dt => new TimeOnly(dt.TimeOfDay.Ticks));

    private sealed class ProviderValueComparerImpl() : ValueComparer<DateTime>(true);
    private sealed class ValueComparerImpl() : ValueComparer<TimeOnly>(true);
}

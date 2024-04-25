using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeLongTypeMapping : LongTypeMapping
{
    public new static SnowflakeLongTypeMapping Default { get; } = new();

    public SnowflakeLongTypeMapping(int? precision = null)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(long),
                    jsonValueReaderWriter: JsonInt64ReaderWriter.Instance
                ),
                storeType: SnowflakeStoreTypeNames.Number,
                precision: precision ?? SnowflakeStoreTypeNames.GetIntegerTypePrecisionToHoldEverything(SignedIntegerType.Long),
                scale: SnowflakeStoreTypeNames.IntegerTypeScale,
                dbType: System.Data.DbType.Int64,
                storeTypePostfix: StoreTypePostfix.PrecisionAndScale))
    {
    }

    protected SnowflakeLongTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeLongTypeMapping(parameters);
    }
}

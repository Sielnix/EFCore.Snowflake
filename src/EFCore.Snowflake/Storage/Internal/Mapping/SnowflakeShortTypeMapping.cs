using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeShortTypeMapping : ShortTypeMapping
{
    public new static SnowflakeShortTypeMapping Default { get; } = new();

    public SnowflakeShortTypeMapping(int? precision = null)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(short),
                    jsonValueReaderWriter: JsonInt16ReaderWriter.Instance
                ),
                storeType: SnowflakeStoreTypeNames.Number,
                precision: precision ?? SnowflakeStoreTypeNames.GetIntegerTypePrecisionToHoldEverything(SignedIntegerType.Short),
                scale: SnowflakeStoreTypeNames.IntegerTypeScale,
                dbType: System.Data.DbType.Int16,
                storeTypePostfix: StoreTypePostfix.PrecisionAndScale))
    {
    }

    protected SnowflakeShortTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeShortTypeMapping(parameters);
    }
}

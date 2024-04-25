using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeIntTypeMapping : IntTypeMapping
{
    public new static SnowflakeIntTypeMapping Default { get; } = new();

    public SnowflakeIntTypeMapping(int? precision = null)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(int),
                    jsonValueReaderWriter: JsonInt32ReaderWriter.Instance
                ),
                storeType: SnowflakeStoreTypeNames.Number,
                precision: precision ?? SnowflakeStoreTypeNames.GetIntegerTypePrecisionToHoldEverything(SignedIntegerType.Int),
                scale: SnowflakeStoreTypeNames.IntegerTypeScale,
                dbType: System.Data.DbType.Int32,
                storeTypePostfix: StoreTypePostfix.PrecisionAndScale))
    {
    }

    protected SnowflakeIntTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeIntTypeMapping(parameters);
    }
}

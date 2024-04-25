using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeFloatAsNumberTypeMapping : DoubleTypeMapping
{
    public new static SnowflakeFloatAsNumberTypeMapping Default { get; } = new();

    public SnowflakeFloatAsNumberTypeMapping(int? precision = null, int? scale = null)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(float),
                    jsonValueReaderWriter: JsonFloatReaderWriter.Instance
                ),
                storeType: SnowflakeStoreTypeNames.Number,
                precision: precision ?? SnowflakeStoreTypeNames.MaxNumberSize,
                scale: scale ?? SnowflakeStoreTypeNames.DefaultScaleForDecimal,
                dbType: System.Data.DbType.Single,
                storeTypePostfix: StoreTypePostfix.PrecisionAndScale))
    {
    }

    protected SnowflakeFloatAsNumberTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeFloatAsNumberTypeMapping(parameters);
    }
}

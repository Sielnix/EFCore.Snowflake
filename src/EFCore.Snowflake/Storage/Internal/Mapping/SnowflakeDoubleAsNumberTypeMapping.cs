using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace EFCore.Snowflake.Storage.Internal.Mapping;
public class SnowflakeDoubleAsNumberTypeMapping : DoubleTypeMapping
{
    public new static SnowflakeDoubleAsNumberTypeMapping Default { get; } = new();

    public SnowflakeDoubleAsNumberTypeMapping(int? precision = null, int? scale = null)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(double),
                    jsonValueReaderWriter: JsonDoubleReaderWriter.Instance
                ),
                storeType: SnowflakeStoreTypeNames.Number,
                precision: precision ?? SnowflakeStoreTypeNames.MaxNumberSize,
                scale: scale ?? SnowflakeStoreTypeNames.DefaultScaleForDecimal,
                dbType: System.Data.DbType.Double,
                storeTypePostfix: StoreTypePostfix.PrecisionAndScale))
    {
    }

    protected SnowflakeDoubleAsNumberTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeDoubleAsNumberTypeMapping(parameters);
    }
}

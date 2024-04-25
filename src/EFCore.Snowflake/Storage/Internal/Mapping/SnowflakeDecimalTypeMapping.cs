using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeDecimalTypeMapping : DecimalTypeMapping
{
    public new static readonly SnowflakeDecimalTypeMapping Default = new(
        SnowflakeStoreTypeNames.Number,
        precision: SnowflakeStoreTypeNames.MaxNumberSize,
        scale: SnowflakeStoreTypeNames.DefaultScaleForDecimal);

    public SnowflakeDecimalTypeMapping(
        string storeType,
        int? precision = null,
        int? scale = null)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(decimal),
                    jsonValueReaderWriter: JsonDecimalReaderWriter.Instance
                ),
                storeType: storeType,
                dbType: System.Data.DbType.Decimal,
                precision: precision,
                scale: scale,
                storeTypePostfix: StoreTypePostfix.PrecisionAndScale))
    {
    }
    
    protected SnowflakeDecimalTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeDecimalTypeMapping(parameters);
    }
}

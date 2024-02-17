using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeVariantTypeMapping : SnowflakeSemiStructuredTypeMapping
{
    public new static readonly SnowflakeVariantTypeMapping Default = new();
    
    public SnowflakeVariantTypeMapping()
        : base(SnowflakeStoreTypeNames.Variant)
    {
    }

    protected SnowflakeVariantTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    public override string InsertWrapFunction => "TO_VARIANT";

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeVariantTypeMapping(parameters);
    }
}

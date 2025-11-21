using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeStructuralJsonTypeMapping : SnowflakeSemiStructuredTypeMapping
{
    public new static readonly SnowflakeStructuralJsonTypeMapping Default = new();
    public SnowflakeStructuralJsonTypeMapping()
        : base(SnowflakeStoreTypeNames.Variant, typeof(JsonTypePlaceholder))
    {
    }

    protected SnowflakeStructuralJsonTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeStructuralJsonTypeMapping(parameters);
    }
}

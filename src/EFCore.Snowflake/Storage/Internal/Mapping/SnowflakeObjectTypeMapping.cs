using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeObjectTypeMapping : SnowflakeSemiStructuredTypeMapping
{
    public new static readonly SnowflakeObjectTypeMapping Default = new();

    public SnowflakeObjectTypeMapping()
        : base(SnowflakeStoreTypeNames.Object)
    {
    }

    protected SnowflakeObjectTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeObjectTypeMapping(parameters);
    }
}

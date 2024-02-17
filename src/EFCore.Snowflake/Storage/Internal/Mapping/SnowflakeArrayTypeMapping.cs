using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeArrayTypeMapping : SnowflakeSemiStructuredTypeMapping
{
    public new static readonly SnowflakeArrayTypeMapping Default = new();

    public SnowflakeArrayTypeMapping()
        : base(SnowflakeStoreTypeNames.Array)
    {
    }

    protected SnowflakeArrayTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeArrayTypeMapping(parameters);
    }
}

using System.Text.Json;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeJsonTypeMapping : SnowflakeSemiStructuredTypeMapping
{
    public new static readonly SnowflakeJsonTypeMapping Default = new();

    public SnowflakeJsonTypeMapping()
        : base(SnowflakeStoreTypeNames.Object, typeof(JsonElement))
    {
    }

    protected SnowflakeJsonTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeJsonTypeMapping(parameters);
    }
}

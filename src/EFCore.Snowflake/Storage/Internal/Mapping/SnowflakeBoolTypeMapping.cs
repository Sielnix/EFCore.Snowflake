using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

internal class SnowflakeBoolTypeMapping : BoolTypeMapping
{
    public new static SnowflakeBoolTypeMapping Default { get; } = new();

    public SnowflakeBoolTypeMapping()
        : base(SnowflakeStoreTypeNames.Boolean)
    {
    }

    protected SnowflakeBoolTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeBoolTypeMapping(parameters);
    }

    protected override string GenerateNonNullSqlLiteral(object value)
        => (bool)value ? "true" : "false";
}

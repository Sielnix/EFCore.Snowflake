using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage;

internal class SnowflakeRelationalCommandBuilderFactory(RelationalCommandBuilderDependencies dependencies)
    : RelationalCommandBuilderFactory(dependencies)
{
    public override IRelationalCommandBuilder Create()
    {
        return new SnowflakeRelationalCommandBuilder(Dependencies);
    }
}

using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage;

public class SnowflakeRelationalCommandBuilder : RelationalCommandBuilder
{
    public SnowflakeRelationalCommandBuilder(RelationalCommandBuilderDependencies dependencies)
        : base(dependencies)
    {
    }

    public override IRelationalCommand Build()
    {
        return new SnowflakeRelationalCommand(Dependencies, ToString(), Parameters);
    }
}

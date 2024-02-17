using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage;
internal class SnowflakeRelationalCommandBuilder : RelationalCommandBuilder
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

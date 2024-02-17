using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EFCore.Snowflake.Metadata.Conventions;

public class SnowflakeConventionSetBuilder : RelationalConventionSetBuilder
{
    public SnowflakeConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override ConventionSet CreateConventionSet()
    {
        ConventionSet set = base.CreateConventionSet();

        set.Add(new SnowflakeValueGenerationStrategyConvention());
        set.Add(new SnowflakeIndexHandlingConvention());
        set.Replace<CascadeDeleteConvention>(new SnowflakeOnDeleteConvention(Dependencies));
        set.Remove(typeof(ForeignKeyIndexConvention));

        return set;
    }
}

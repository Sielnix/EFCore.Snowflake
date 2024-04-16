using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EFCore.Snowflake.Metadata.Conventions;

public class SnowflakeConventionSetBuilder(
    ProviderConventionSetBuilderDependencies dependencies,
    RelationalConventionSetBuilderDependencies relationalDependencies)
    : RelationalConventionSetBuilder(dependencies, relationalDependencies)
{
    public override ConventionSet CreateConventionSet()
    {
        ConventionSet set = base.CreateConventionSet();

        set.Add(new SnowflakeValueGenerationStrategyConvention());
        set.Add(new SnowflakeIndexHandlingConvention());

        set.Replace<CascadeDeleteConvention>(new SnowflakeOnDeleteConvention(Dependencies));

        set.Remove(typeof(ForeignKeyIndexConvention));

        set.Replace<RuntimeModelConvention>(new SnowflakeRuntimeModelConvention(Dependencies, RelationalDependencies));
        set.Replace<StoreGenerationConvention>(new SnowflakeStoreGenerationConvention(Dependencies, RelationalDependencies));
        set.Replace<ValueGenerationConvention>(new SnowflakeValueGenerationConvention(Dependencies, RelationalDependencies));

        return set;
    }
}

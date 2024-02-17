using EFCore.Snowflake.Storage.Internal;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EFCore.Snowflake.Metadata.Conventions;

internal class SnowflakeConventionSetBuilder : RelationalConventionSetBuilder
{
    private readonly string? _schemaInConnectionString;

    public SnowflakeConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies,
        ISnowflakeConnection connection)
        : base(dependencies, relationalDependencies)
    {
        _schemaInConnectionString = connection.SchemaInConnectionString;
    }

    public override ConventionSet CreateConventionSet()
    {
        ConventionSet set = base.CreateConventionSet();

        set.Add(new SnowflakeValueGenerationStrategyConvention());
        set.Add(new SnowflakeDefaultSchemaConvention(_schemaInConnectionString));
        set.Add(new SnowflakeIndexHandlingConvention());
        set.Replace<CascadeDeleteConvention>(new SnowflakeOnDeleteConvention(Dependencies));
        set.Remove(typeof(ForeignKeyIndexConvention));

        return set;
    }
}

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;
using EFCore.Snowflake.Infrastructure;
using EFCore.Snowflake.Storage.Internal;

namespace EFCore.Snowflake.FunctionalTests;

public class SnowflakeApiConsistencyTest(SnowflakeApiConsistencyTest.SnowflakeApiConsistencyFixture fixture)
    : ApiConsistencyTestBase<SnowflakeApiConsistencyTest.SnowflakeApiConsistencyFixture>(fixture)
{
    protected override void AddServices(ServiceCollection serviceCollection)
        => serviceCollection.AddEntityFrameworkSnowflake();

    protected override Assembly TargetAssembly
        => typeof(SnowflakeConnection).Assembly;
    
    public class SnowflakeApiConsistencyFixture : ApiConsistencyFixtureBase
    {
        public override HashSet<Type> FluentApiTypes { get; } = new()
        {
            typeof(SnowflakeDbContextOptionsBuilder),
            typeof(SnowflakeDbContextOptionsBuilderExtensions),
            typeof(SnowflakeMigrationBuilderExtensions),
            typeof(SnowflakeModelBuilderExtensions),
            typeof(SnowflakePropertyBuilderExtensions),
            typeof(SnowflakeServiceCollectionExtensions),
        };

        public override
            Dictionary<Type,
                (Type ReadonlyExtensions,
                Type MutableExtensions,
                Type ConventionExtensions,
                Type ConventionBuilderExtensions,
                Type? RuntimeExtensions)> MetadataExtensionTypes
        { get; }
            = new()
            {
                {
                    typeof(IReadOnlyModel), (
                        typeof(SnowflakeModelExtensions),
                        typeof(SnowflakeModelExtensions),
                        typeof(SnowflakeModelExtensions),
                        typeof(SnowflakeModelBuilderExtensions),
                        null
                    )
                },
                {
                    typeof(IReadOnlyProperty), (
                        typeof(SnowflakePropertyExtensions),
                        typeof(SnowflakePropertyExtensions),
                        typeof(SnowflakePropertyExtensions),
                        typeof(SnowflakePropertyBuilderExtensions),
                        null
                    )
                },
            };
    }
}

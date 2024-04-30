using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Snowflake.Metadata.Conventions;

public class SnowflakeConventionSetBuilder(
    ProviderConventionSetBuilderDependencies dependencies,
    RelationalConventionSetBuilderDependencies relationalDependencies)
    : RelationalConventionSetBuilder(dependencies, relationalDependencies)
{
    public override ConventionSet CreateConventionSet()
    {
        ConventionSet set = base.CreateConventionSet();

        set.Add(new SnowflakeValueGenerationStrategyConvention(Dependencies, RelationalDependencies));
        set.Add(new SnowflakeIndexHandlingConvention());
        set.Add(new SnowflakeSequenceOrderConvention());

        set.Replace<CascadeDeleteConvention>(new SnowflakeOnDeleteConvention(Dependencies));

        set.Remove(typeof(ForeignKeyIndexConvention));

        set.Replace<RuntimeModelConvention>(new SnowflakeRuntimeModelConvention(Dependencies, RelationalDependencies));
        set.Replace<StoreGenerationConvention>(new SnowflakeStoreGenerationConvention(Dependencies, RelationalDependencies));
        set.Replace<ValueGenerationConvention>(new SnowflakeValueGenerationConvention(Dependencies, RelationalDependencies));

        return set;
    }

    /// <summary>
    ///     Call this method to build a <see cref="ModelBuilder" /> for SQL Server outside of <see cref="DbContext.OnModelCreating" />.
    /// </summary>
    /// <remarks>
    ///     Note that it is unusual to use this method. Consider using <see cref="DbContext" /> in the normal way instead.
    /// </remarks>
    /// <returns>The convention set.</returns>
    public static ModelBuilder CreateModelBuilder()
    {
        using var serviceScope = CreateServiceScope();
        using var context = serviceScope.ServiceProvider.GetRequiredService<DbContext>();
        return new ModelBuilder(ConventionSet.CreateConventionSet(context), context.GetService<ModelDependencies>());
    }

    private static IServiceScope CreateServiceScope()
    {
        var serviceProvider = new ServiceCollection()
            .AddEntityFrameworkSnowflake()
            .AddDbContext<DbContext>(
                (p, o) =>
                    o.UseSnowflake("Server=.")
                        .UseInternalServiceProvider(p))
            .BuildServiceProvider();

        return serviceProvider.GetRequiredService<IServiceScopeFactory>().CreateScope();
    }
}

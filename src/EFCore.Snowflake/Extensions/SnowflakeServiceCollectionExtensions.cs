using EFCore.Snowflake.Diagnostics.Internal;
using EFCore.Snowflake.Infrastructure;
using EFCore.Snowflake.Infrastructure.Internal;
using EFCore.Snowflake.Metadata.Conventions;
using EFCore.Snowflake.Metadata.Internal;
using EFCore.Snowflake.Migrations;
using EFCore.Snowflake.Migrations.Internal;
using EFCore.Snowflake.Query.ExpressionTranslators;
using EFCore.Snowflake.Query.Internal;
using EFCore.Snowflake.Storage;
using EFCore.Snowflake.Storage.Internal;
using EFCore.Snowflake.Storage.Internal.Mapping;
using EFCore.Snowflake.Update;
using EFCore.Snowflake.Update.Internal;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;
using System.ComponentModel;
using Microsoft.EntityFrameworkCore.Query.Internal;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;
public static class SnowflakeServiceCollectionExtensions
{
    public static IServiceCollection AddSnowflake<TContext>(
        this IServiceCollection serviceCollection,
        string? connectionString,
        Action<SnowflakeDbContextOptionsBuilder>? snowflakeOptionsAction = null,
        Action<DbContextOptionsBuilder>? optionsAction = null)
        where TContext : DbContext
        => serviceCollection.AddDbContext<TContext>(
            (_, options) =>
            {
                optionsAction?.Invoke(options);
                options.UseSnowflake(connectionString, snowflakeOptionsAction);
            });

    /// <summary>
    ///     <para>
    ///         Adds the services required by the Snowflake database provider for Entity Framework
    ///         to an <see cref="IServiceCollection" />.
    ///     </para>
    ///     <para>
    ///         Warning: Do not call this method accidentally. It is much more likely you need
    ///         to call <see cref="AddSnowflake{TContext}" />.
    ///     </para>
    /// </summary>
    /// <remarks>
    ///     Calling this method is no longer necessary when building most applications, including those that
    ///     use dependency injection in ASP.NET or elsewhere.
    ///     It is only needed when building the internal service provider for use with
    ///     the <see cref="DbContextOptionsBuilder.UseInternalServiceProvider" /> method.
    ///     This is not recommend other than for some advanced scenarios.
    /// </remarks>
    /// <param name="serviceCollection">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <returns>
    ///     The same service collection so that multiple calls can be chained.
    /// </returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static IServiceCollection AddEntityFrameworkSnowflake(this IServiceCollection serviceCollection)
    {
        Check.NotNull(serviceCollection, nameof(serviceCollection));

        new EntityFrameworkRelationalServicesBuilder(serviceCollection)
            .TryAdd<LoggingDefinitions, SnowflakeLoggingDefinitions>()
            .TryAdd<IDatabaseProvider, DatabaseProvider<SnowflakeOptionsExtension>>()
            .TryAdd<IRelationalTypeMappingSource, SnowflakeTypeMappingSource>()
            .TryAdd<IProviderConventionSetBuilder, SnowflakeConventionSetBuilder>()
            .TryAdd<ISqlGenerationHelper, SnowflakeSqlGenerationHelper>()
            .TryAdd<IRelationalAnnotationProvider, SnowflakeAnnotationProvider>()
            .TryAdd<IRelationalTransactionFactory, SnowflakeRelationalTransactionFactory>()
            .TryAdd<IMigrator, SnowflakeMigrator>()
            .TryAdd<IModificationCommandBatchFactory, SnowflakeModificationCommandBatchFactory>()
            .TryAdd<IRelationalConnection>(p => p.GetRequiredService<ISnowflakeConnection>())
            .TryAdd<ICommandBatchPreparer, SnowflakeCommandBatchPreparer>()
            .TryAdd<IMigrationsSqlGenerator, SnowflakeMigrationsSqlGenerator>()
            .TryAdd<IRelationalDatabaseCreator, SnowflakeDatabaseCreator>()
            .TryAdd<IHistoryRepository, SnowflakeHistoryRepository>()
            .TryAdd<IAggregateMethodCallTranslatorProvider, SnowflakeAggregateMethodCallTranslatorProvider>()
            .TryAdd<IMemberTranslatorProvider, SnowflakeMemberTranslatorProvider>()
            .TryAdd<IMethodCallTranslatorProvider, SnowflakeMethodCallTranslatorProvider>()
            .TryAdd<IUpdateSqlGenerator, SnowflakeUpdateSqlGenerator>()
            .TryAdd<IQuerySqlGeneratorFactory, SnowflakeQuerySqlGeneratorFactory>()
            .TryAdd<IQueryCompilationContextFactory, SnowflakeQueryCompilationContextFactory>()
            .TryAdd<IRelationalSqlTranslatingExpressionVisitorFactory, SnowflakeSqlTranslatingExpressionVisitorFactory>()
            .TryAdd<IRelationalCommandBuilderFactory, SnowflakeRelationalCommandBuilderFactory>()
            .TryAddProviderSpecificServices(b => b
                .TryAddScoped<ISnowflakeConnection, SnowflakeConnection>())
            .TryAddCoreServices();

        return serviceCollection;
    }
}

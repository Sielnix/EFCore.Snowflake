using System.Data.Common;
using EFCore.Snowflake.Infrastructure;
using EFCore.Snowflake.Infrastructure.Internal;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore.Infrastructure;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

public static class SnowflakeDbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseSnowflake(
        this DbContextOptionsBuilder optionsBuilder,
        Action<SnowflakeDbContextOptionsBuilder>? snowflakeOptionsAction = null)
    {
        Check.NotNull(optionsBuilder, nameof(optionsBuilder));

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(GetOrCreateExtension(optionsBuilder));

        ConfigureWarnings(optionsBuilder);

        snowflakeOptionsAction?.Invoke(new SnowflakeDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseSnowflake(
        this DbContextOptionsBuilder optionsBuilder,
        string? connectionString,
        Action<SnowflakeDbContextOptionsBuilder>? snowflakeOptionsAction = null)
    {
        Check.NotNull(optionsBuilder, nameof(optionsBuilder));
        SnowflakeOptionsExtension extension = (SnowflakeOptionsExtension)GetOrCreateExtension(optionsBuilder).WithConnectionString(connectionString);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        snowflakeOptionsAction?.Invoke(new SnowflakeDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseSnowflake(
        this DbContextOptionsBuilder optionsBuilder,
        DbConnection connection,
        Action<SnowflakeDbContextOptionsBuilder>? snowflakeOptionsAction = null)
        => UseSnowflake(optionsBuilder, connection, contextOwnsConnection: false, snowflakeOptionsAction);

    public static DbContextOptionsBuilder UseSnowflake(
        this DbContextOptionsBuilder optionsBuilder,
        DbConnection connection,
        bool contextOwnsConnection,
        Action<SnowflakeDbContextOptionsBuilder>? snowflakeOptionsAction = null)
    {
        Check.NotNull(connection, nameof(connection));

        var extension = (SnowflakeOptionsExtension)GetOrCreateExtension(optionsBuilder).WithConnection(connection, contextOwnsConnection);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        snowflakeOptionsAction?.Invoke(new SnowflakeDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder<TContext> UseSnowflake<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        Action<SnowflakeDbContextOptionsBuilder>? snowflakeOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseSnowflake(
            (DbContextOptionsBuilder)optionsBuilder, snowflakeOptionsAction);

    public static DbContextOptionsBuilder<TContext> UseSnowflake<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string? connectionString,
        Action<SnowflakeDbContextOptionsBuilder>? snowflakeOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseSnowflake(
            (DbContextOptionsBuilder)optionsBuilder, connectionString, snowflakeOptionsAction);

    public static DbContextOptionsBuilder<TContext> UseSnowflake<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        DbConnection connection,
        Action<SnowflakeDbContextOptionsBuilder>? snowflakeOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseSnowflake(
            (DbContextOptionsBuilder)optionsBuilder, connection, snowflakeOptionsAction);

    public static DbContextOptionsBuilder<TContext> UseSnowflake<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        DbConnection connection,
        bool contextOwnsConnection,
        Action<SnowflakeDbContextOptionsBuilder>? snowflakeOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseSnowflake(
            (DbContextOptionsBuilder)optionsBuilder, connection, contextOwnsConnection, snowflakeOptionsAction);

    private static SnowflakeOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
    {
        return optionsBuilder.Options.FindExtension<SnowflakeOptionsExtension>() is { } existing
            ? new SnowflakeOptionsExtension(existing)
            : new SnowflakeOptionsExtension();
    }

    private static void ConfigureWarnings(DbContextOptionsBuilder optionsBuilder)
    {
        var coreOptionsExtension = optionsBuilder.Options.FindExtension<CoreOptionsExtension>()
            ?? new CoreOptionsExtension();

        coreOptionsExtension = RelationalOptionsExtension.WithDefaultWarningConfiguration(coreOptionsExtension);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(coreOptionsExtension);
    }
}

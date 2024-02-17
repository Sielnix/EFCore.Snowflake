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
        Action<SnowflakeDbContextOptionsBuilder>? npgsqlOptionsAction = null)
        => UseSnowflake(optionsBuilder, connection, contextOwnsConnection: false, npgsqlOptionsAction);

    public static DbContextOptionsBuilder UseSnowflake(
        this DbContextOptionsBuilder optionsBuilder,
        DbConnection connection,
        bool contextOwnsConnection,
        Action<SnowflakeDbContextOptionsBuilder>? snowflakeOptionsAction = null)
    {
        Check.NotNull(connection, nameof(connection));

        var extension = (SnowflakeOptionsExtension)GetOrCreateExtension(optionsBuilder).WithConnection(connection, contextOwnsConnection);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        snowflakeOptionsAction?.Invoke(new SnowflakeDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

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

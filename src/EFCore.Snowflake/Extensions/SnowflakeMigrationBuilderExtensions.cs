using EFCore.Snowflake.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Migrations;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

public static class SnowflakeMigrationBuilderExtensions
{
    public static bool IsSnowflake(this MigrationBuilder migrationBuilder)
        => string.Equals(
            migrationBuilder.ActiveProvider,
            typeof(SnowflakeOptionsExtension).Assembly.GetName().Name,
            StringComparison.Ordinal);
}

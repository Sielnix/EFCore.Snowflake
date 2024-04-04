using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Internal;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Migrations.Internal;
public class SnowflakeMigrator(
    IMigrationsAssembly migrationsAssembly,
    IHistoryRepository historyRepository,
    IDatabaseCreator databaseCreator,
    IMigrationsSqlGenerator migrationsSqlGenerator,
    IRawSqlCommandBuilder rawSqlCommandBuilder,
    IMigrationCommandExecutor migrationCommandExecutor,
    IRelationalConnection connection,
    ISqlGenerationHelper sqlGenerationHelper,
    ICurrentDbContext currentContext,
    IModelRuntimeInitializer modelRuntimeInitializer,
    IDiagnosticsLogger<DbLoggerCategory.Migrations> logger,
    IRelationalCommandDiagnosticsLogger commandLogger,
    IDatabaseProvider databaseProvider)
    : Migrator(migrationsAssembly,
        historyRepository,
        databaseCreator,
        migrationsSqlGenerator,
        rawSqlCommandBuilder,
        migrationCommandExecutor,
        connection,
        sqlGenerationHelper,
        currentContext,
        modelRuntimeInitializer,
        logger,
        commandLogger,
        databaseProvider)
{
    public override string GenerateScript(
        string? fromMigration = null,
        string? toMigration = null,
        MigrationsSqlGenerationOptions options = MigrationsSqlGenerationOptions.Default)
    {
        // Snowflake isn't working well with transactions and scripts $$
        options |= MigrationsSqlGenerationOptions.NoTransactions;

        return base.GenerateScript(fromMigration, toMigration, options);
    }
}

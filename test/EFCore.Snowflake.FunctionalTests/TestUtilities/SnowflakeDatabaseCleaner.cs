using System.Diagnostics;
using EFCore.Snowflake.Diagnostics.Internal;
using EFCore.Snowflake.Migrations.Operations;
using EFCore.Snowflake.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics.Internal;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.Logging;

namespace EFCore.Snowflake.FunctionalTests.TestUtilities;

public class SnowflakeDatabaseCleaner : RelationalDatabaseCleaner
{
    private readonly ISqlGenerationHelper _sqlGenerationHelper;

    public SnowflakeDatabaseCleaner(ISqlGenerationHelper sqlGenerationHelper)
    {
        _sqlGenerationHelper = sqlGenerationHelper;
    }

    protected override IDatabaseModelFactory CreateDatabaseModelFactory(ILoggerFactory loggerFactory)
    {
        return new SnowflakeDatabaseModelFactory(
            _sqlGenerationHelper,
            new DiagnosticsLogger<DbLoggerCategory.Scaffolding>(
                loggerFactory,
                new LoggingOptions(),
                new DiagnosticListener("Fake"),
                new SnowflakeLoggingDefinitions(),
                new NullDbContextLogger()));
    }

    protected override MigrationOperation Drop(DatabaseTable table)
    {
        if (table is DatabaseView)
        {
            return new SnowflakeDropViewOperation { Name = table.Name, Schema = table.Schema };
        }

        return base.Drop(table);
    }
}

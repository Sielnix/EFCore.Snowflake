using EFCore.Snowflake.Storage.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Snowflake.Data.Client;

namespace EFCore.Snowflake.Migrations.Internal;

public class SnowflakeHistoryRepository : HistoryRepository, IHistoryRepository
{
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(1);
    
    public SnowflakeHistoryRepository(HistoryRepositoryDependencies dependencies)
        : base(dependencies)
    {
    }

    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Explicit;

    private RelationalTypeMapping StringMapping => Dependencies.TypeMappingSource.GetMapping(typeof(string));

    protected virtual string LockTableName { get; } = "__EFMigrationsLock";

    protected override string? TableSchema
    {
        get
        {
            string? baseSchema = base.TableSchema;
            if (baseSchema != null)
            {
                return baseSchema;
            }

            var connection = (ISnowflakeConnection)Dependencies.Connection;
            return connection.SchemaInConnectionString;
        }
    }

    protected override string ExistsSql
    {
        get
        {
            string query = $"""
    SELECT TO_BOOLEAN(COUNT(1)) FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA  = {StringMapping.GenerateSqlLiteral(TableSchema ?? "PUBLIC")} AND TABLE_NAME = {StringMapping.GenerateSqlLiteral(TableName)};
""";

            return query;
        }
    }

    public override string GetCreateScript()
    {
        string script = base.GetCreateScript();

        string query = $"""
EXECUTE IMMEDIATE
$$
BEGIN

    {script}
    
END;
$$;

""";

        return query;
    }

    protected override bool InterpretExistsResult(object? value)
    {
        return (bool?)value == true;
    }

    public override string GetCreateIfNotExistsScript()
    {
        string script = GetCreateScript();

        const string createTable = "CREATE TABLE";

        return script.Insert(script.IndexOf(createTable, StringComparison.Ordinal) + createTable.Length, " IF NOT EXISTS");
    }

    public override string GetBeginIfNotExistsScript(string migrationId)
    {
        string query = $"""
EXECUTE IMMEDIATE
$$
DECLARE
	row_exists BOOLEAN;
BEGIN
	SELECT 
		TO_BOOLEAN(COUNT(1))
	INTO
		row_exists
	FROM {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)} 
		WHERE {SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName)} = {StringMapping.GenerateSqlLiteral(migrationId)};
	
    IF (row_exists = false) THEN 
""";

        return query;
    }

    public override string GetBeginIfExistsScript(string migrationId)
    {
        string query = $"""
EXECUTE IMMEDIATE
$$
DECLARE
	row_exists BOOLEAN;
BEGIN
	SELECT 
		TO_BOOLEAN(COUNT(1))
	INTO
		row_exists
	FROM {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)} 
		WHERE {SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName)} = {StringMapping.GenerateSqlLiteral(migrationId)};
	
    IF (row_exists) THEN 
""";

        return query;
    }

    public override string GetEndIfScript()
    {
        const string query = """
    END IF;
END;
$$;

""";

        return query;
    }
    
    public override IMigrationsDatabaseLock AcquireDatabaseLock()
    {
        Dependencies.MigrationsLogger.AcquiringMigrationLock();

        if (!Dependencies.DatabaseCreator.Exists())
        {
            return new SnowflakeMigrationEmptyDatabaseLock(this);
        }


        TimeSpan retryDelay = RetryDelay;
        while (true)
        {
            object? tableExistsResult = CreateLockTableExistsCommand().ExecuteScalar(CreateRelationalCommandParameters());
            bool tableExists = InterpretExistsResult(tableExistsResult);
            if (!tableExists)
            {
                try
                {
                    CreateLockTableCommand().ExecuteNonQuery(CreateRelationalCommandParameters());
                }
                catch (SnowflakeDbException e) when (e.Message.Contains("already exists"))
                {
                    continue;
                }

                return CreateMigrationDatabaseLock();
            }

            Thread.Sleep(retryDelay);
            retryDelay = retryDelay.Add(retryDelay);

            if (retryDelay > MaxRetryDelay)
            {
                retryDelay = MaxRetryDelay;
            }
        }
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public override async Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(
        CancellationToken cancellationToken = default)
    {
        Dependencies.MigrationsLogger.AcquiringMigrationLock();

        if (!await Dependencies.DatabaseCreator.ExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return new SnowflakeMigrationEmptyDatabaseLock(this);
        }

        TimeSpan retryDelay = RetryDelay;
        while (true)
        {
            object? tableExistsResult = await CreateLockTableExistsCommand().ExecuteScalarAsync(CreateRelationalCommandParameters(), cancellationToken).ConfigureAwait(false);
            bool tableExists = InterpretExistsResult(tableExistsResult);
            if (!tableExists)
            {
                try
                {
                    await CreateLockTableCommand().ExecuteNonQueryAsync(CreateRelationalCommandParameters(), cancellationToken).ConfigureAwait(false);
                }
                catch (SnowflakeDbException e) when (e.Message.Contains("already exists"))
                {
                    continue;
                }

                return CreateMigrationDatabaseLock();
            }

            await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
            retryDelay = retryDelay.Add(retryDelay);

            if (retryDelay > MaxRetryDelay)
            {
                retryDelay = MaxRetryDelay;
            }
        }
    }

    bool IHistoryRepository.CreateIfNotExists()
        => Dependencies.MigrationCommandExecutor.ExecuteNonQuery(
               GetCreateIfNotExistsCommands(), Dependencies.Connection, new MigrationExecutionState(), commitTransaction: true)
           != 0;

    async Task<bool> IHistoryRepository.CreateIfNotExistsAsync(CancellationToken cancellationToken)
        => (await Dependencies.MigrationCommandExecutor.ExecuteNonQueryAsync(
               GetCreateIfNotExistsCommands(), Dependencies.Connection, new MigrationExecutionState(), commitTransaction: true, cancellationToken: cancellationToken).ConfigureAwait(false))
           != 0;

    private IReadOnlyList<MigrationCommand> GetCreateIfNotExistsCommands()
        => Dependencies.MigrationsSqlGenerator.Generate([new SqlOperation
        {
            Sql = GetCreateIfNotExistsScript(),
            SuppressTransaction = true
        }]);

    private IRelationalCommand CreateLockTableCommand()
        => Dependencies.RawSqlCommandBuilder.Build(
            $"""
CREATE TABLE {SqlGenerationHelper.DelimitIdentifier(LockTableName, TableSchema)} (
    "Id" VARCHAR NOT NULL PRIMARY KEY
);
""");

    private IRelationalCommand CreateDeleteLockCommand()
        => Dependencies.RawSqlCommandBuilder.Build(
            $"""
             DROP TABLE {SqlGenerationHelper.DelimitIdentifier(LockTableName, TableSchema)};
             """);

    private IRelationalCommand CreateLockTableExistsCommand()
    {
        string sql = $"""
                      SELECT 
                          TO_BOOLEAN(COUNT(1))
                      FROM
                          INFORMATION_SCHEMA.TABLES
                          WHERE TABLE_SCHEMA = {StringMapping.GenerateSqlLiteral(TableSchema)} AND TABLE_NAME = {StringMapping.GenerateSqlLiteral(LockTableName)};
                      """;
        return Dependencies.RawSqlCommandBuilder.Build(sql);
    }

    private SnowflakeMigrationDatabaseLock CreateMigrationDatabaseLock()
        => new(CreateDeleteLockCommand(), CreateRelationalCommandParameters(), this);

    private RelationalCommandParameterObject CreateRelationalCommandParameters()
        => new(
            Dependencies.Connection,
            null,
            null,
            Dependencies.CurrentContext.Context,
            Dependencies.CommandLogger, CommandSource.Migrations);

}

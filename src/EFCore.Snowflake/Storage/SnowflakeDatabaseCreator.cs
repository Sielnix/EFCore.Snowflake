using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.Migrations.Operations;
using EFCore.Snowflake.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Snowflake.Data.Client;
using System.Transactions;

namespace EFCore.Snowflake.Storage;

public class SnowflakeDatabaseCreator : RelationalDatabaseCreator
{
    private const int DbNotExistsOrNotAuthorizedErrorCode = 390201;

    private readonly ISnowflakeConnection _snowflakeConnection;
    private readonly IRawSqlCommandBuilder _rawSqlCommandBuilder;

    public SnowflakeDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        ISnowflakeConnection snowflakeConnection,
        IRawSqlCommandBuilder rawSqlCommandBuilder)
        : base(dependencies)
    {
        _snowflakeConnection = snowflakeConnection;
        _rawSqlCommandBuilder = rawSqlCommandBuilder;
    }

    public override bool Exists()
    {
        using TransactionScope _ = new(TransactionScopeOption.Suppress);
        try
        {
            ISnowflakeConnection checkConnection = _snowflakeConnection.CopyConnection();
            using (checkConnection)
            {
                checkConnection.Open(true);
            }

            return true;
        }
        catch (Exception e)
        {
            if (IsNoDbException(e))
            {
                return false;
            }

            throw;
        }
    }

    public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
    {
        using TransactionScope _ = new(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        try
        {
            ISnowflakeConnection checkConnection = _snowflakeConnection.CopyConnection();
            await using (checkConnection.ConfigureAwait(false))
            {
                await checkConnection.OpenAsync(cancellationToken, true).ConfigureAwait(false);
            }

            return true;
        }
        catch (Exception e)
        {
            if (IsNoDbException(e))
            {
                return false;
            }

            throw;
        }
    }

    public override bool HasTables()
    {
        return Dependencies.ExecutionStrategy.Execute(
            _snowflakeConnection,
            connection =>
            {
                IRelationalCommand command = CreateHasTablesCommand();
                object? result = command.ExecuteScalar(
                    new RelationalCommandParameterObject(
                        connection,
                        null,
                        null,
                        Dependencies.CurrentContext.Context,
                        Dependencies.CommandLogger,
                        CommandSource.Migrations));

                if (result == null)
                {
                    throw new InvalidOperationException("Unexpected null result");
                }

                return (bool)result;
            },
            null);
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public override async Task<bool> HasTablesAsync(CancellationToken cancellationToken = default)
    {
        return await Dependencies.ExecutionStrategy.ExecuteAsync(
            _snowflakeConnection,
            async (connection, ct) =>
            {
                IRelationalCommand command = CreateHasTablesCommand();
                object? result = await command.ExecuteScalarAsync(
                    new RelationalCommandParameterObject(
                        connection,
                        null,
                        null,
                        Dependencies.CurrentContext.Context,
                        Dependencies.CommandLogger,
                        CommandSource.Migrations),
                    cancellationToken: ct);

                if (result == null)
                {
                    throw new InvalidOperationException("Unexpected null result");
                }

                return (bool)result;
            },
            null,
            cancellationToken)
            .ConfigureAwait(false);
    }
        
    public override void Create()
    {
        using (ISnowflakeConnection adminConnection = _snowflakeConnection.CreateAdminConnection())
        {
            Dependencies.MigrationCommandExecutor
                .ExecuteNonQuery(CreateCreateOperations(), adminConnection);
        }

        ClearPool();

        Exists();
    }

    public override async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        ISnowflakeConnection adminConnection = _snowflakeConnection.CreateAdminConnection();
        await using (adminConnection.ConfigureAwait(false))
        {
            await Dependencies.MigrationCommandExecutor
                .ExecuteNonQueryAsync(CreateCreateOperations(), adminConnection, cancellationToken)
                .ConfigureAwait(false);
        }

        ClearPool();

        await ExistsAsync(cancellationToken).ConfigureAwait(false);
    }

    public override async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        ClearPool();
        ISnowflakeConnection adminConnection = _snowflakeConnection.CreateAdminConnection();
        await using var _ = adminConnection.ConfigureAwait(false);
        await Dependencies.MigrationCommandExecutor
            .ExecuteNonQueryAsync(CreateDropCommands(), adminConnection, cancellationToken)
            .ConfigureAwait(false);
    }

    public override void Delete()
    {
        ClearPool();

        ISnowflakeConnection adminConnection = _snowflakeConnection.CreateAdminConnection();
        Dependencies.MigrationCommandExecutor
            .ExecuteNonQuery(CreateDropCommands(), adminConnection);
    }

    private bool IsNoDbException(Exception e)
    {
        return e.GetAllExceptions().Any(ie =>
            ie is SnowflakeDbException { ErrorCode: DbNotExistsOrNotAuthorizedErrorCode });
    }

    private IReadOnlyList<MigrationCommand> CreateCreateOperations()
    {
        IModel designTimeModel = Dependencies.CurrentContext.Context.GetService<IDesignTimeModel>().Model;

        string databaseName = _snowflakeConnection.DatabaseInConnectionString
                              ?? throw new InvalidOperationException("Database name is not set");

        List<MigrationOperation> migrationCommands =
        [
            new SnowflakeCreateDatabaseOperation()
            {
                Name = databaseName,
                Collation = designTimeModel.GetRelationalModel().Collation,
            },
        ];

        string? schema = _snowflakeConnection.SchemaInConnectionString;
        if (schema != null && schema != "PUBLIC")
        {
            migrationCommands.Add(new EnsureSchemaOperation()
            {
                Name = schema
            });
        }

        return Dependencies.MigrationsSqlGenerator.Generate(migrationCommands);
    }

    private IReadOnlyList<MigrationCommand> CreateDropCommands()
    {
        string databaseName = _snowflakeConnection.DatabaseInConnectionString
                              ?? throw new InvalidOperationException("Database name is not set");

        MigrationOperation[] operations = [new SnowflakeDropDatabaseOperation { Name = databaseName }];

        return Dependencies.MigrationsSqlGenerator.Generate(operations);
    }

    private IRelationalCommand CreateHasTablesCommand()
        => _rawSqlCommandBuilder
            .Build(@"SELECT CASE WHEN COUNT(*) = 0 THEN FALSE ELSE TRUE END
FROM information_schema.tables 
WHERE table_type = 'BASE TABLE';");

    private void ClearPool()
    {
        SnowflakeDbConnectionPool.ClearAllPools();
    }
}

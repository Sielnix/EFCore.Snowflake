using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Snowflake.Data.Client;
using System.Data.Common;

namespace EFCore.Snowflake.Storage.Internal;

public interface ISnowflakeConnection : IRelationalConnection
{
    string? SchemaInConnectionString { get; }
    string? DatabaseInConnectionString { get; }
    ISnowflakeConnection CopyConnection();
    ISnowflakeConnection CreateAdminConnection();
}

public class SnowflakeConnection : RelationalConnection, ISnowflakeConnection
{
    private const int DefaultAdminConnectionCommandTimeout = 60;

    private readonly ISqlGenerationHelper _sqlGenerationHelper;

    private bool _isPopulated;
    private string? _schema;
    private string? _database;

    public SnowflakeConnection(
        RelationalConnectionDependencies dependencies,
        ISqlGenerationHelper sqlGenerationHelper)
        : base(dependencies)
    {
        _sqlGenerationHelper = sqlGenerationHelper;
    }

    public string? SchemaInConnectionString
    {
        get
        {
            if (!_isPopulated)
            {
                PopulateConnectionStringOptions();
            }

            return _schema;
        }
    }

    public string? DatabaseInConnectionString
    {
        get
        {
            if (!_isPopulated)
            {
                PopulateConnectionStringOptions();
            }

            return _database;
        }
    }
    public ISnowflakeConnection CopyConnection()
    {
        return new SnowflakeConnection(Dependencies, _sqlGenerationHelper);
    }

    public ISnowflakeConnection CreateAdminConnection()
    {
        SnowflakeDbConnectionStringBuilder builder = new();
        builder.ConnectionString = GetValidatedConnectionString();
        builder.Remove("db");
        builder.Remove("schema");

        DbContextOptionsBuilder optionsBuilder = new DbContextOptionsBuilder()
            .UseSnowflake(
                builder.ConnectionString,
                b => b.CommandTimeout(CommandTimeout ?? DefaultAdminConnectionCommandTimeout));

        return new SnowflakeConnection(
            Dependencies with { ContextOptions = optionsBuilder.Options },
            _sqlGenerationHelper);
    }

    private new SnowflakeDbConnection DbConnection => (SnowflakeDbConnection)base.DbConnection;

    protected override DbConnection CreateDbConnection()
    {
        return new SnowflakeDbConnection(GetValidatedConnectionString());
    }

    protected override void OpenDbConnection(bool errorsExpected)
    {
        base.OpenDbConnection(errorsExpected);

        //ExecuteSql(GetUseDatabaseQuery());

        if (
            DatabaseInConnectionString != null
            && SchemaInConnectionString != null
            && SchemaInConnectionString != "PUBLIC")
        {
            ExecuteSql(GetUseSchemaQuery());
        }
    }

    private void ExecuteSql(string? sql)
    {
        if (sql is null)
        {
            return;
        }

        using DbCommand command = DbConnection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private async Task ExecuteSqlAsync(string? sql, CancellationToken ct)
    {
        if (sql is null)
        {
            return;
        }

        DbCommand command = DbConnection.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    protected override async Task OpenDbConnectionAsync(bool errorsExpected, CancellationToken cancellationToken)
    {
        await base.OpenDbConnectionAsync(errorsExpected, cancellationToken).ConfigureAwait(false);

        //await ExecuteSqlAsync(GetUseDatabaseQuery(), cancellationToken).ConfigureAwait(false);

        if (
            DatabaseInConnectionString != null
            && SchemaInConnectionString != null
            && SchemaInConnectionString != "PUBLIC")
        {
            await ExecuteSqlAsync(GetUseSchemaQuery(), cancellationToken).ConfigureAwait(false);
        }
        //
    }

    private string? GetUseDatabaseQuery()
    {
        return null;
        //string? database = DatabaseInConnectionString;
        //return database is not null ? $"USE DATABASE {_sqlGenerationHelper.DelimitIdentifier(DatabaseInConnectionString)}" : null;
    }

    private string GetUseSchemaQuery()
    {
        return $"USE SCHEMA {_sqlGenerationHelper.DelimitIdentifier(SchemaInConnectionString!)}";
    }

    private void PopulateConnectionStringOptions()
    {
        SnowflakeDbConnectionStringBuilder connectionStringBuilder = new();
        connectionStringBuilder.ConnectionString = ConnectionString;

        const string schemaName = "schema";
        const string databaseName = "db";

        if (connectionStringBuilder.TryGetValue(schemaName, out object? schemaNameValue))
        {
            _schema = schemaNameValue.ToString();
        }

        if (connectionStringBuilder.TryGetValue(databaseName, out object? dbNameValue))
        {
            _database = dbNameValue.ToString();
        }

        _isPopulated = true;
    }
}

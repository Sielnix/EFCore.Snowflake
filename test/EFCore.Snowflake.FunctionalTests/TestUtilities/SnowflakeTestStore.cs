using EFCore.Snowflake.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Snowflake.Data.Client;
using System.Data.Common;
using EFCore.Snowflake.Storage;

namespace EFCore.Snowflake.FunctionalTests.TestUtilities;

public class SnowflakeTestStore : RelationalTestStore
{
    private readonly string? _schemaOverride;

    public SnowflakeTestStore(string name, string? schemaOverride, bool shared)
        : base(name, shared, new SnowflakeDbConnection(CreateConnectionString(name, schemaOverride)))
    {
        _schemaOverride = schemaOverride;
    }

    public static SnowflakeTestStore Create(string name, string? schemaOverride) => new(name, schemaOverride, shared: false);

    public static Task<SnowflakeTestStore> CreateInitializedAsync(string name, string? schemaOverride = null)
        => new SnowflakeTestStore(name, schemaOverride, shared: false)
            .InitializeSnowflakeAsync(null, null, null);

    public static SnowflakeTestStore GetOrCreate(string name, string? schemaOverride) => new(name, schemaOverride, shared: true);

    public int ExecuteNonQuery(string sql, params object[] parameters)
        => ExecuteNonQuery(Connection, sql);

    private static int ExecuteNonQuery(DbConnection connection, string sql)
        => Execute(connection, command => command.ExecuteNonQuery(), sql, false);

    public Task<int> ExecuteNonQueryAsync(string sql, params object[] parameters)
        => ExecuteNonQueryAsync(Connection, sql);

    private static Task<int> ExecuteNonQueryAsync(DbConnection connection, string sql)
        => ExecuteAsync(connection, command => command.ExecuteNonQueryAsync(), sql, false);

    public T? ExecuteScalar<T>(string sql, params object[] parameters)
        => ExecuteScalar<T>(Connection, sql, parameters);

    private static T? ExecuteScalar<T>(DbConnection connection, string sql, params object[] parameters)
        => Execute(connection, command => (T?)command.ExecuteScalar(), sql, false);

    public async Task<SnowflakeTestStore> InitializeSnowflakeAsync(
        IServiceProvider? serviceProvider,
        Func<DbContext>? createContext,
        Func<DbContext, Task>? seed)
        => (SnowflakeTestStore)await InitializeAsync(serviceProvider, createContext, seed);

    public static T Execute<T>(
        DbConnection connection,
        Func<DbCommand, T> execute,
        string sql,
        bool useTransaction)
    {
        if (connection.State != System.Data.ConnectionState.Closed)
        {
            connection.Close();
        }

        connection.Open();
        try
        {
            using var transaction = useTransaction ? connection.BeginTransaction() : null;
            T result;
            using (var command = CreateCommand(connection, sql))
            {
                command.Transaction = transaction;
                result = execute(command);
            }

            transaction?.Commit();

            return result;
        }
        finally
        {
            if (connection.State != System.Data.ConnectionState.Closed)
            {
                connection.Close();
            }
        }
    }

    protected override async Task InitializeAsync(Func<DbContext> createContext, Func<DbContext, Task>? seed, Func<DbContext, Task>? clean)
    {
        await CreateDatabase(clean);

        await using var context = createContext();
        await context.Database.EnsureCreatedResilientlyAsync();

        if (seed != null)
        {
            await seed(context);
        }
    }

    private async Task CreateDatabase(Func<DbContext, Task>? clean)
    {
        await using SnowflakeDbConnection connection = new(CreateConnectionString(null));
        await connection.OpenAsync();

        if (await DatabaseExists(connection))
        {
            await using var context = new DbContext(
                AddProviderOptions(new DbContextOptionsBuilder().EnableServiceProviderCaching(false)).Options);
            await CleanAsync(context);

            if (clean != null)
            {
                await clean(context);
            }
        }
        else
        {
            await ExecuteNonQueryAsync(connection, $"CREATE DATABASE {ToDbLiteral(ToDbName(Name))}");
            if (_schemaOverride != null)
            {
                await using SnowflakeDbConnection connectionForSchema = new(CreateConnectionString(Name));
                await ExecuteNonQueryAsync(connectionForSchema, $"CREATE SCHEMA IF NOT EXISTS {ToDbLiteral(ToDbName(_schemaOverride))}");
            }
        }
    }

    private static string ToDbLiteral(string name)
    {
        return $"\"{name.Replace("\"", "\"\"")}\"";
    }

    private async Task<bool> DatabaseExists(SnowflakeDbConnection connection)
    {
        try
        {
            DbCommand dbExistsCommand = connection.CreateCommand();
            string dbName = ToDbName(Name);
            dbExistsCommand.CommandText =
                $@"SHOW DATABASES STARTS WITH '{SnowflakeStringLikeEscape.EscapeSqlLiteral(dbName)}';";
            await using var reader = await dbExistsCommand.ExecuteReaderAsync();
            IEnumerable<(DbColumn c, int i)> cols = reader.GetColumnSchema().Select((c, i) => (c, i));
            int nameColOrdinal = cols.Single(c => c.c.ColumnName == "name").i;

            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                if (reader.GetString(nameColOrdinal) == dbName)
                {
                    return true;
                }
            }
        }
        catch (Exception e)
        {
            if (SnowflakeDatabaseCreator.IsNoDbException(e))
            {
                return false;
            }

            throw;
        }
        
        return false;
    }


    private static async Task<T> ExecuteAsync<T>(
        DbConnection connection,
        Func<DbCommand, Task<T>> executeAsync,
        string sql,
        bool useTransaction = false)
    {
        if (connection.State != System.Data.ConnectionState.Closed)
        {
            await connection.CloseAsync();
        }

        await connection.OpenAsync();
        try
        {
            await using var transaction = useTransaction ? await connection.BeginTransactionAsync() : null;
            T result;
            await using (var command = CreateCommand(connection, sql))
            {
                result = await executeAsync(command);
            }

            if (transaction != null)
            {
                await transaction.CommitAsync();
            }

            return result;
        }
        finally
        {
            if (connection.State != System.Data.ConnectionState.Closed)
            {
                await connection.CloseAsync();
            }
        }
    }


    private static DbCommand CreateCommand(
        DbConnection connection,
        string commandText)
    {
        var command = connection.CreateCommand();

        command.CommandText = commandText;

        return command;
    }

    public override DbContextOptionsBuilder AddProviderOptions(DbContextOptionsBuilder builder)
    {
        return builder.UseSnowflake(Connection, opt => opt.ApplyConfiguration());
    }

    public override Task CleanAsync(DbContext context)
    {
        context.Database.EnsureClean();
        return Task.CompletedTask;
    }

    public static string CreateConnectionString(string? name, string? schemaOverride = null)
    {
        SnowflakeDbConnectionStringBuilder builder = new();
        builder.ConnectionString = TestEnvironment.DefaultConnectionString;

        if (name != null)
        {
            builder["db"] = ToDbName(name);
        }

        if (schemaOverride != null)
        {
            builder["schema"] = schemaOverride;
        }

        return builder.ToString();
    }

    private static string ToDbName(string name)
    {
        return name.ToUpperInvariant();
    }
}

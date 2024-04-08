using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Snowflake.Data.Client;

namespace EFCore.Snowflake.FunctionalTests.TestUtilities;

public class SnowflakeTestStore : RelationalTestStore
{
    public SnowflakeTestStore(
        string name,
        string? schemaOverride,
        bool shared)
        : base(name, shared)
    {
        string connectionString = CreateConnectionString(name, schemaOverride);

        this.ConnectionString = connectionString;
        this.Connection = new SnowflakeDbConnection(connectionString);
    }

    public static SnowflakeTestStore Create(string name, string? schemaOverride) => new(name, schemaOverride, shared: false);

    public static SnowflakeTestStore CreateInitialized(string name, string? schemaOverride = null)
        => new SnowflakeTestStore(name, schemaOverride, shared: false)
            .InitializeSnowflake(null, null, null);

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

    public SnowflakeTestStore InitializeSnowflake(
        IServiceProvider? serviceProvider,
        Func<DbContext>? createContext,
        Action<DbContext>? seed)
        => (SnowflakeTestStore)Initialize(serviceProvider, createContext, seed);

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

    public override void Clean(DbContext context)
    {
        context.Database.EnsureClean();
    }

    public static string CreateConnectionString(string name, string? schemaOverride = null)
    {
        SnowflakeDbConnectionStringBuilder builder = new();
        builder.ConnectionString = TestEnvironment.DefaultConnectionString;
        builder["db"] = name.ToUpperInvariant();

        if (schemaOverride != null)
        {
            builder["schema"] = schemaOverride;
        }

        return builder.ToString();
    }
}

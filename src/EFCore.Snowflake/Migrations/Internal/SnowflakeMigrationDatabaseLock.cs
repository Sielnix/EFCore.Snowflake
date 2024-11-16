using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Migrations.Internal;

public class SnowflakeMigrationDatabaseLock(
    IRelationalCommand releaseLockCommand,
    RelationalCommandParameterObject relationalCommandParameters,
    IHistoryRepository historyRepository,
    CancellationToken cancellationToken = default)
    : IMigrationsDatabaseLock
{
    public virtual IHistoryRepository HistoryRepository => historyRepository;

    public void Dispose()
        => releaseLockCommand.ExecuteScalar(relationalCommandParameters);

    public async ValueTask DisposeAsync()
        => await releaseLockCommand.ExecuteScalarAsync(relationalCommandParameters, cancellationToken).ConfigureAwait(false);
}

public class SnowflakeMigrationEmptyDatabaseLock(IHistoryRepository historyRepository) : IMigrationsDatabaseLock
{
    public void Dispose()
    {
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    public IHistoryRepository HistoryRepository => historyRepository;
}

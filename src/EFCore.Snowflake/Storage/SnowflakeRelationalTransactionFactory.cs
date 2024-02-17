using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage;

public class SnowflakeRelationalTransactionFactory : RelationalTransactionFactory
{
    public SnowflakeRelationalTransactionFactory(RelationalTransactionFactoryDependencies dependencies)
        : base(dependencies)
    {
    }

    public override RelationalTransaction Create(
        IRelationalConnection connection,
        DbTransaction transaction,
        Guid transactionId,
        IDiagnosticsLogger<DbLoggerCategory.Database.Transaction> logger,
        bool transactionOwned)
    {
        return new SnowflakeRelationalTransaction(
            connection,
            transaction,
            transactionId,
            logger,
            transactionOwned,
            Dependencies.SqlGenerationHelper);
    }
}

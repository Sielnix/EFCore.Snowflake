using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage;

public class SnowflakeRelationalTransaction : RelationalTransaction
{
    public SnowflakeRelationalTransaction(
        IRelationalConnection connection,
        DbTransaction transaction,
        Guid transactionId,
        IDiagnosticsLogger<DbLoggerCategory.Database.Transaction> logger,
        bool transactionOwned,
        ISqlGenerationHelper sqlGenerationHelper)
        : base(connection, transaction, transactionId, logger, transactionOwned, sqlGenerationHelper)
    {
    }

    public override bool SupportsSavepoints => false;
}

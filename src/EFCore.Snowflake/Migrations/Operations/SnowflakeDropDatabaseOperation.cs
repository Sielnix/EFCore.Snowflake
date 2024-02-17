using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EFCore.Snowflake.Migrations.Operations;

public class SnowflakeDropDatabaseOperation : MigrationOperation
{
    public virtual required string Name { get; set; }
}

using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EFCore.Snowflake.Migrations.Operations;

public class SnowflakeCreateDatabaseOperation : DatabaseOperation
{
    public virtual required string Name { get; set; }
}

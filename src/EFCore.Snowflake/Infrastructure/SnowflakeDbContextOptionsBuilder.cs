using EFCore.Snowflake.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EFCore.Snowflake.Infrastructure;
public class SnowflakeDbContextOptionsBuilder : RelationalDbContextOptionsBuilder<SnowflakeDbContextOptionsBuilder, SnowflakeOptionsExtension>
{
    public SnowflakeDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        : base(optionsBuilder)
    {
    }
}

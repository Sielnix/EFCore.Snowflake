using EFCore.Snowflake.Infrastructure;
using Microsoft.EntityFrameworkCore;

namespace EFCore.Snowflake.FunctionalTests.TestUtilities;

public static class SnowflakeDbContextOptionsBuilderExtensions
{
    public static SnowflakeDbContextOptionsBuilder ApplyConfiguration(this SnowflakeDbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseQuerySplittingBehavior(QuerySplittingBehavior.SingleQuery);

        return optionsBuilder;
    }
}

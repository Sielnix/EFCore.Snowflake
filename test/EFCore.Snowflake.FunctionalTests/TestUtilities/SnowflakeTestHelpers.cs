using EFCore.Snowflake.Diagnostics.Internal;
using EFCore.Snowflake.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Snowflake.Data.Client;

namespace EFCore.Snowflake.FunctionalTests.TestUtilities;

public class SnowflakeTestHelpers : RelationalTestHelpers
{
    public static SnowflakeTestHelpers Instance { get; } = new();

    public override IServiceCollection AddProviderServices(IServiceCollection services)
    {
        return services.AddEntityFrameworkSnowflake();
    }

    public override DbContextOptionsBuilder UseProviderOptions(DbContextOptionsBuilder optionsBuilder)
    {
        return optionsBuilder.UseSnowflake(new SnowflakeDbConnection("Database=DummyDatabase"));
    }

    public override LoggingDefinitions LoggingDefinitions { get; } = new SnowflakeLoggingDefinitions();
}

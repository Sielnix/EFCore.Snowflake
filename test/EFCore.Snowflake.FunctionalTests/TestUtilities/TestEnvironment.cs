using Microsoft.Extensions.Configuration;

namespace EFCore.Snowflake.FunctionalTests.TestUtilities;
public static class TestEnvironment
{
    public static IConfiguration Config { get; } = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("config.json", optional: true)
        .AddJsonFile("config.local.json", optional: true)
        .AddEnvironmentVariables()
        .Build()
        .GetSection("Test:Snowflake");

    public static string DefaultConnectionString => Config["DefaultConnectionString"] ?? throw new InvalidOperationException("Connection string is not provided");
}

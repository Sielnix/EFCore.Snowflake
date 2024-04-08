using EFCore.Snowflake.Extensions;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Snowflake.FunctionalTests.TestUtilities;
internal class SnowflakeTestStoreFactory : RelationalTestStoreFactory
{
    private readonly string? _schemaOverride;
    public static SnowflakeTestStoreFactory Instance { get; } = new();

    public SnowflakeTestStoreFactory(string? schemaOverride = null)
    {
        _schemaOverride = schemaOverride;
    }

    public override TestStore Create(string storeName)
    {
        return SnowflakeTestStore.Create(storeName, _schemaOverride);
    }

    public override TestStore GetOrCreate(string storeName)
    {
        return SnowflakeTestStore.GetOrCreate(storeName, _schemaOverride);
    }

    public override IServiceCollection AddProviderServices(IServiceCollection serviceCollection)
    {
        return serviceCollection.AddEntityFrameworkSnowflake();
    }
}

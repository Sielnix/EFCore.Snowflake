using EFCore.Snowflake.Extensions;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Snowflake.FunctionalTests.TestUtilities;
internal class SnowflakeTestStoreFactory : RelationalTestStoreFactory
{
    public static SnowflakeTestStoreFactory Instance { get; } = new();

    public override TestStore Create(string storeName)
    {
        return SnowflakeTestStore.Create(storeName);
    }

    public override TestStore GetOrCreate(string storeName)
    {
        return SnowflakeTestStore.GetOrCreate(storeName);
    }

    public override IServiceCollection AddProviderServices(IServiceCollection serviceCollection)
    {
        return serviceCollection.AddEntityFrameworkSnowflake();
    }
}

using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Snowflake.Design.Internal;

public class SnowflakeDesignTimeServices : IDesignTimeServices
{
    public void ConfigureDesignTimeServices(IServiceCollection serviceCollection)
    {
        serviceCollection.AddEntityFrameworkSnowflake();
        new EntityFrameworkRelationalDesignServicesBuilder(serviceCollection)
            .TryAdd<IDatabaseModelFactory, SnowflakeDatabaseModelFactory>()
            .TryAdd<IProviderConfigurationCodeGenerator, SnowflakeCodeGenerator>()
            .TryAddCoreServices();
    }
}

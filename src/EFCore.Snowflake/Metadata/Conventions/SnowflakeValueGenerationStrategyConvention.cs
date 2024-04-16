using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EFCore.Snowflake.Metadata.Conventions;

public class SnowflakeValueGenerationStrategyConvention : IModelInitializedConvention
{
    public void ProcessModelInitialized(IConventionModelBuilder modelBuilder, IConventionContext<IConventionModelBuilder> context)
    {
         modelBuilder.HasValueGenerationStrategy(SnowflakeValueGenerationStrategy.AutoIncrement);
    }
}

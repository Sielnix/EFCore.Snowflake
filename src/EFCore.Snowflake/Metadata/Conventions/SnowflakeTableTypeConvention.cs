using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EFCore.Snowflake.Metadata.Conventions;

public class SnowflakeTableTypeConvention : IModelFinalizingConvention
{
    public void ProcessModelFinalizing(IConventionModelBuilder modelBuilder, IConventionContext<IConventionModelBuilder> context)
    {
        var entityTypes = modelBuilder.Metadata.GetEntityTypes();
        foreach (var entityType in entityTypes)
        {
            if (entityType[RelationalAnnotationNames.ViewName] == null)
            {
                entityType.Builder.HasTableType(SnowflakeTableType.Permanent);
            }
        }
    }
}

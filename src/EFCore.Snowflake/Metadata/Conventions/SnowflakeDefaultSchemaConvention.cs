using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EFCore.Snowflake.Metadata.Conventions;

internal class SnowflakeDefaultSchemaConvention : IModelFinalizingConvention
{
    private readonly string? _schemaInConnectionString;

    public SnowflakeDefaultSchemaConvention(string? schemaInConnectionString)
    {
        _schemaInConnectionString = schemaInConnectionString;
    }

    public void ProcessModelFinalizing(IConventionModelBuilder modelBuilder, IConventionContext<IConventionModelBuilder> context)
    {
        //if (!string.IsNullOrEmpty(modelBuilder.Metadata.GetDefaultSchema()))
        //{
        //    return;
        //}

        //// try get schema and add
        //if (_schemaInConnectionString is not null)
        //{
        //    modelBuilder.HasDefaultSchema(_schemaInConnectionString);
        //}
    }
}

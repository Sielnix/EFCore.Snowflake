using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EFCore.Snowflake.Metadata.Conventions;

public class SnowflakeOnDeleteConvention : CascadeDeleteConvention
{
    public SnowflakeOnDeleteConvention(ProviderConventionSetBuilderDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override DeleteBehavior GetTargetDeleteBehavior(IConventionForeignKey foreignKey)
    {
        return DeleteBehavior.NoAction;
    }
}

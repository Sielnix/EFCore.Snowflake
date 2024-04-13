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
        // if we would use DeleteBehavior.Cascade (as it is in default implementation) then
        // this foreign key wouldn't be created at all by Snowflake
        // todo: check this behavior on hybrid tables
        return DeleteBehavior.NoAction;
    }
}

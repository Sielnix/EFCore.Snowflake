using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;

namespace EFCore.Snowflake.FunctionalTests;


/// <summary>
///       WARNING!
/// This test or it's dependency (.UseModel()) in EFCORE is bugged
/// See: https://github.com/dotnet/efcore/issues/34032
/// There's a workaround for that in <see cref="EFCore.Snowflake.Migrations.SnowflakeMigrationsSqlGenerator.GetTableType"/>
/// </summary>
public class DataBindingSnowflakeTest(F1SnowflakeFixture fixture) : DataBindingTestBase<F1SnowflakeFixture>(fixture)
{
    public override void Entity_removed_from_navigation_property_binding_list_is_removed_from_nav_property_but_not_marked_Deleted(
        CascadeTiming deleteOrphansTiming)
    {
        if (deleteOrphansTiming == CascadeTiming.Immediate)
        {
            // TODO: this test should be moved to Hybrid tables, that contains ForeignKeys
            return;
        }

        base.Entity_removed_from_navigation_property_binding_list_is_removed_from_nav_property_but_not_marked_Deleted(deleteOrphansTiming);
    }
}

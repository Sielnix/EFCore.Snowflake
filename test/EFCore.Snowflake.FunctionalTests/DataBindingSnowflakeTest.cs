using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;

namespace EFCore.Snowflake.FunctionalTests;

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

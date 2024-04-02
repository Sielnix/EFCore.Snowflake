using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;
public class FieldsOnlyLoadSnowflakeTest(FieldsOnlyLoadSnowflakeTest.FieldsOnlyLoadSnowflakeFixture fixture)
    : FieldsOnlyLoadTestBase<FieldsOnlyLoadSnowflakeTest.FieldsOnlyLoadSnowflakeFixture>(fixture)
{
    public class FieldsOnlyLoadSnowflakeFixture : FieldsOnlyLoadFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }

    // TODO: run tests below over hybrid tables, as they support indexes with cascade behavior
    public override async Task Load_one_to_one_PK_to_PK_reference_to_dependent_already_loaded(EntityState state,
        bool async)
    {
        if (state == EntityState.Deleted)
        {
            return;
        }

        await base.Load_one_to_one_PK_to_PK_reference_to_dependent_already_loaded(state, async);
    }

    public override async Task Load_one_to_one_PK_to_PK_reference_to_dependent_using_Query_already_loaded(
        EntityState state, bool async)
    {
        if (state == EntityState.Deleted)
        {
            return;
        }

        await base.Load_one_to_one_PK_to_PK_reference_to_dependent_using_Query_already_loaded(state, async);
    }
}

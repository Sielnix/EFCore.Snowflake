using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class LoadSnowflakeTest : LoadTestBase<LoadSnowflakeTest.LoadSnowflakeFixture>
{
    public LoadSnowflakeTest(LoadSnowflakeFixture fixture)
        : base(fixture)
    {
        fixture.TestSqlLoggerFactory.Clear();
    }

    public override void Lazy_load_one_to_one_PK_to_PK_reference_to_principal_already_loaded(
        EntityState state,
        QueryTrackingBehavior queryTrackingBehavior)
    {
        if (state == EntityState.Deleted && queryTrackingBehavior == QueryTrackingBehavior.TrackAll)
        {
            // todo: use hybrid tables
            return;
        }

        base.Lazy_load_one_to_one_PK_to_PK_reference_to_principal_already_loaded(state, queryTrackingBehavior);
    }

    public override async Task Load_one_to_one_PK_to_PK_reference_to_principal_already_loaded(EntityState state, bool async)
    {
        if (state == EntityState.Deleted)
        {
            return;
        }

        await base.Load_one_to_one_PK_to_PK_reference_to_principal_already_loaded(state, async);
    }

    public override async Task Load_one_to_one_PK_to_PK_reference_to_principal_using_Query_already_loaded(EntityState state, bool async)
    {
        if (state == EntityState.Deleted)
        {
            return;
        }

        await base.Load_one_to_one_PK_to_PK_reference_to_principal_using_Query_already_loaded(state, async);
    }

    [ConditionalFact(Skip = "Use hybrid tables")]
    public override void Setting_navigation_to_null_is_detected_by_local_DetectChanges()
    {
        base.Setting_navigation_to_null_is_detected_by_local_DetectChanges();
    }

    public class LoadSnowflakeFixture : LoadFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

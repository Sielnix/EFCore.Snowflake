using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;
public class LazyLoadProxySnowflakeTest : LazyLoadProxyTestBase<LazyLoadProxySnowflakeTest.LoadSnowflakeFixture>
{
    public LazyLoadProxySnowflakeTest(LoadSnowflakeFixture fixture)
        : base(fixture)
    {
        fixture.TestSqlLoggerFactory.Clear();
    }

    [ConditionalFact(Skip = "Invalid Assert usage, missing Snowflake amount of decimal places storage")]
    public override void Can_serialize_proxies_to_JSON()
    {
        base.Can_serialize_proxies_to_JSON();
    }

    public override void Lazy_load_one_to_one_PK_to_PK_reference_to_dependent_already_loaded(EntityState state)
    {
        if (state == EntityState.Deleted)
        {
            // todo: move to hybrid table with FKeys and test there
            return;
        }

        base.Lazy_load_one_to_one_PK_to_PK_reference_to_dependent_already_loaded(state);
    }

    private string? Sql { get; set; }

    public class LoadSnowflakeFixture : LoadFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

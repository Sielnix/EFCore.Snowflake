using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.Extensions.Logging;

namespace EFCore.Snowflake.FunctionalTests;
public abstract class FindSnowflakeTest : FindTestBase<FindSnowflakeTest.FindSnowflakeFixture>
{
    protected FindSnowflakeTest(FindSnowflakeFixture fixture)
        : base(fixture)
    {
        fixture.TestSqlLoggerFactory.Clear();
    }

    public class FindSnowflakeTestSet(FindSnowflakeFixture fixture) : FindSnowflakeTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaSetFinder();
    }

    public class FindSnowflakeTestContext(FindSnowflakeFixture fixture) : FindSnowflakeTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaContextFinder();
    }

    public class FindSnowflakeTestNonGeneric(FindSnowflakeFixture fixture) : FindSnowflakeTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaNonGenericContextFinder();
    }

    public class FindSnowflakeFixture : FindFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ServiceProvider.GetRequiredService<ILoggerFactory>();

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

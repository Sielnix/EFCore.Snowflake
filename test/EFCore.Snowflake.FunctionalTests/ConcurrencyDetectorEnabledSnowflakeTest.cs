using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class ConcurrencyDetectorEnabledSnowflakeTest : ConcurrencyDetectorEnabledRelationalTestBase<
    ConcurrencyDetectorEnabledSnowflakeTest.ConcurrencyDetectorSnowflakeFixture>
{
    public ConcurrencyDetectorEnabledSnowflakeTest(ConcurrencyDetectorSnowflakeFixture fixture)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }

    protected override async Task ConcurrencyDetectorTest(Func<ConcurrencyDetectorDbContext, Task<object>> test)
    {
        await base.ConcurrencyDetectorTest(test);

        Assert.Empty(Fixture.TestSqlLoggerFactory.SqlStatements);
    }

    public class ConcurrencyDetectorSnowflakeFixture : ConcurrencyDetectorFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;
    }
}

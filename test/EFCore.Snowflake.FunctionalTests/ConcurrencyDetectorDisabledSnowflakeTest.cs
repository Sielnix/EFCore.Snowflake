using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class ConcurrencyDetectorDisabledSnowflakeTest : ConcurrencyDetectorDisabledRelationalTestBase<
    ConcurrencyDetectorDisabledSnowflakeTest.ConcurrencyDetectorSnowflakeFixture>
{
    public ConcurrencyDetectorDisabledSnowflakeTest(ConcurrencyDetectorSnowflakeFixture fixture)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }

    protected override async Task ConcurrencyDetectorTest(Func<ConcurrencyDetectorDbContext, Task<object>> test)
    {
        await base.ConcurrencyDetectorTest(test);

        Assert.NotEmpty(Fixture.TestSqlLoggerFactory.SqlStatements);
    }

    public class ConcurrencyDetectorSnowflakeFixture : ConcurrencyDetectorFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
            => builder.EnableThreadSafetyChecks(enableChecks: false);
    }
}

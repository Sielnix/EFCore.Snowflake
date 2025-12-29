using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class EntitySplittingQuerySnowflakeTest(NonSharedFixture fixture) : EntitySplittingQueryTestBase(fixture)
{
    protected override ITestStoreFactory TestStoreFactory => SnowflakeTestStoreFactory.Instance;

    protected new void AssertSql(params string[] expected)
    {
        TestSqlLoggerFactory.AssertSql(expected);
    }
}

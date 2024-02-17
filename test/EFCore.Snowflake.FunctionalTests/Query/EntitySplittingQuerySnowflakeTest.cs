using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class EntitySplittingQuerySnowflakeTest : EntitySplittingQueryTestBase
{
    protected override ITestStoreFactory TestStoreFactory => SnowflakeTestStoreFactory.Instance;

    protected new void AssertSql(params string[] expected)
    {
        TestSqlLoggerFactory.AssertSql(expected);
    }
}

using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests;
public class EntitySplittingSnowflakeTest(ITestOutputHelper testOutputHelper) : EntitySplittingTestBase(new NonSharedFixture(), testOutputHelper)
{
    protected override ITestStoreFactory TestStoreFactory
        => SnowflakeTestStoreFactory.Instance;
}

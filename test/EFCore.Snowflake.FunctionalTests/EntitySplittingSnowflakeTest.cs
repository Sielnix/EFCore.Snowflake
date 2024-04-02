using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests;
public class EntitySplittingSnowflakeTest(ITestOutputHelper testOutputHelper) : EntitySplittingTestBase(testOutputHelper)
{
    protected override ITestStoreFactory TestStoreFactory
        => SnowflakeTestStoreFactory.Instance;
}

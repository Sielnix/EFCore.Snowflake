using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class CompositeKeyEndToEndSnowflakeTest(CompositeKeyEndToEndSnowflakeTest.CompositeKeyEndToEndSnowflakeFixture fixture)
    : CompositeKeyEndToEndTestBase<CompositeKeyEndToEndSnowflakeTest.CompositeKeyEndToEndSnowflakeFixture>(fixture)
{
    public class CompositeKeyEndToEndSnowflakeFixture : CompositeKeyEndToEndFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

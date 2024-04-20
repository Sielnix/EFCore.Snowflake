using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;
public class NotificationEntitiesSnowflakeTest(
    NotificationEntitiesSnowflakeTest.NotificationEntitiesSnowflakeFixture fixture)
    : NotificationEntitiesTestBase<NotificationEntitiesSnowflakeTest.NotificationEntitiesSnowflakeFixture>(fixture)
{
    public class NotificationEntitiesSnowflakeFixture : NotificationEntitiesFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

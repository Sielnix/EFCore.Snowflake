using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class CompositeKeysQuerySnowflakeFixture : CompositeKeysQueryRelationalFixtureBase
{
    protected override ITestStoreFactory TestStoreFactory => SnowflakeTestStoreFactory.Instance;
}

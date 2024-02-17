using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class GearsOfWarQuerySnowflakeFixture : GearsOfWarQueryRelationalFixture
{
    protected override ITestStoreFactory TestStoreFactory => SnowflakeTestStoreFactory.Instance;
}

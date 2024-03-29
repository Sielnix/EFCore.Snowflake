using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;
using System.Reflection;
using EFCore.Snowflake.Design.Internal;
using EFCore.Snowflake.FunctionalTests.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class DesignTimeSnowflakeTest(DesignTimeSnowflakeTest.DesignTimeSnowflakeFixture fixture)
    : DesignTimeTestBase<DesignTimeSnowflakeTest.DesignTimeSnowflakeFixture>(fixture)
{
    protected override Assembly ProviderAssembly
        => typeof(SnowflakeDesignTimeServices).Assembly;

    public class DesignTimeSnowflakeFixture : DesignTimeFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

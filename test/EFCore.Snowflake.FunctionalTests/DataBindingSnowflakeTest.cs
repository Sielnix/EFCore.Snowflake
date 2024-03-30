using Microsoft.EntityFrameworkCore;

namespace EFCore.Snowflake.FunctionalTests;

public class DataBindingSnowflakeTest : DataBindingTestBase<F1SnowflakeFixture>
{
    public DataBindingSnowflakeTest(F1SnowflakeFixture fixture)
        : base(fixture)
    {
    }
}

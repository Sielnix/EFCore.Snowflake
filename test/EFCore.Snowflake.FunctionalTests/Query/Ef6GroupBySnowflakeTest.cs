using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Snowflake.Data.Client;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class Ef6GroupBySnowflakeTest : Ef6GroupByTestBase<Ef6GroupBySnowflakeTest.Ef6GroupBySnowflakeFixture>
{
    public Ef6GroupBySnowflakeTest(Ef6GroupBySnowflakeFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task Whats_new_2021_sample_3(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Whats_new_2021_sample_3(async));
    }
    public override async Task Whats_new_2021_sample_4(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Whats_new_2021_sample_4(async));
    }
    public override async Task Whats_new_2021_sample_5(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Whats_new_2021_sample_5(async));
    }
    public override async Task Whats_new_2021_sample_6(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Whats_new_2021_sample_6(async));
    }

    private void AssertSql(params string[] expected)
    {
        Fixture.TestSqlLoggerFactory.AssertSql(expected);
    }

    public class Ef6GroupBySnowflakeFixture : Ef6GroupByFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override ITestStoreFactory TestStoreFactory => SnowflakeTestStoreFactory.Instance;
    }
}

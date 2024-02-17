using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class FunkyDataQuerySnowflakeTest : FunkyDataQueryTestBase<FunkyDataQuerySnowflakeTest.FunkyDataQuerySnowflakeFixture>
{
    public FunkyDataQuerySnowflakeTest(FunkyDataQuerySnowflakeFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    protected virtual bool CanExecuteQueryString => true;

    public override async Task String_FirstOrDefault_and_LastOrDefault(bool async)
    {
        // More or less this is bug in Snowflake .net connector
        await Assert.ThrowsAsync<IndexOutOfRangeException>(async () =>
            await base.String_FirstOrDefault_and_LastOrDefault(async));
    }

    public class FunkyDataQuerySnowflakeFixture : FunkyDataQueryFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

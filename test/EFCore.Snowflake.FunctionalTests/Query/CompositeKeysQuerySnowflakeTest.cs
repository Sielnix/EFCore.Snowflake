using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class CompositeKeysQuerySnowflakeTest : CompositeKeysQueryRelationalTestBase<CompositeKeysQuerySnowflakeFixture>
{
    public CompositeKeysQuerySnowflakeTest(
        CompositeKeysQuerySnowflakeFixture fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    //protected override bool CanExecuteQueryString => true;

    //private void AssertSql(params string[] expected)
    //{
    //    Fixture.TestSqlLoggerFactory.AssertSql(expected);
    //}
}

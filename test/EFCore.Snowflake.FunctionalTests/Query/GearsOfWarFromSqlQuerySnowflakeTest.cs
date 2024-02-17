using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class GearsOfWarFromSqlQuerySnowflakeTest : GearsOfWarFromSqlQueryTestBase<GearsOfWarQuerySnowflakeFixture>
{
    public GearsOfWarFromSqlQuerySnowflakeTest(GearsOfWarQuerySnowflakeFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        fixture.TestSqlLoggerFactory.Clear();
    }

    public override void From_sql_queryable_simple_columns_out_of_order()
    {
        base.From_sql_queryable_simple_columns_out_of_order();

        Assert.Equal(
            @"SELECT ""Id"", ""Name"", ""IsAutomatic"", ""AmmunitionType"", ""OwnerFullName"", ""SynergyWithId"" FROM ""Weapons"" ORDER BY ""Name""",
            Sql);
    }

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();

    private string Sql
        => Fixture.TestSqlLoggerFactory.Sql;
}

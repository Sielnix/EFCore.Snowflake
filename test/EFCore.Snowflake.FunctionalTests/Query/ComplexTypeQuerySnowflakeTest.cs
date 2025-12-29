using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Query;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class ComplexTypeQuerySnowflakeTest : ComplexTypeQueryRelationalTestBase<ComplexTypeQuerySnowflakeTest.ComplexTypeQuerySnowflakeFixture>
{
    public ComplexTypeQuerySnowflakeTest(ComplexTypeQuerySnowflakeFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task Same_entity_with_complex_type_projected_twice_with_pushdown_as_part_of_another_projection(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(() => base.Same_entity_with_complex_type_projected_twice_with_pushdown_as_part_of_another_projection(async));
    }

    public class ComplexTypeQuerySnowflakeFixture : ComplexTypeQueryRelationalFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory => SnowflakeTestStoreFactory.Instance;
    }

    private void AssertSql(params string[] expected)
    {
        Fixture.TestSqlLoggerFactory.AssertSql(expected);
    }
}

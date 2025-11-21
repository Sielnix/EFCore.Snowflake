using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests;
public class ComplexTypesTrackingSnowflakeTest : ComplexTypesTrackingRelationalTestBase<ComplexTypesTrackingSnowflakeTest.SnowflakeFixture>
{
    public ComplexTypesTrackingSnowflakeTest(SnowflakeFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture, testOutputHelper)
    {
        //fixture.TestSqlLoggerFactory.Clear();
        //fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
        => facade.UseTransaction(transaction.GetDbTransaction());

    public class SnowflakeFixture : RelationalFixtureBase, ITestSqlLoggerFactory
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        //public TestSqlLoggerFactory TestSqlLoggerFactory
        //    => (TestSqlLoggerFactory)ListLoggerFactory;
    }
}


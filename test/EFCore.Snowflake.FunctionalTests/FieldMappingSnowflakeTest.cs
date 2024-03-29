using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;
using EFCore.Snowflake.FunctionalTests.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class FieldMappingSnowflakeTest(FieldMappingSnowflakeTest.FieldMappingSnowflakeFixture fixture)
    : FieldMappingTestBase<FieldMappingSnowflakeTest.FieldMappingSnowflakeFixture>(fixture)
{
    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
        => facade.UseTransaction(transaction.GetDbTransaction());

    public class FieldMappingSnowflakeFixture : FieldMappingFixtureBase
    {
        protected override string StoreName { get; } = "FieldMapping";

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

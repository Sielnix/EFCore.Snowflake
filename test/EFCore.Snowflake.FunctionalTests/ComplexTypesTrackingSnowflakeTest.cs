using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests;
public class ComplexTypesTrackingSnowflakeTest : ComplexTypesTrackingRelationalTestBase<ComplexTypesTrackingSnowflakeTest.SnowflakeFixture>
{
    public ComplexTypesTrackingSnowflakeTest(SnowflakeFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture, testOutputHelper)
    {
    }

    // Custom JSON query translation is not implemented
    public override Task Can_change_state_from_Deleted_with_complex_collection(EntityState newState, bool async)
        => Assert.ThrowsAsync<InvalidOperationException>(() => base.Can_change_state_from_Deleted_with_complex_collection(newState, async));

    public override Task Can_change_state_from_Deleted_with_complex_field_collection(EntityState newState, bool async)
        => Assert.ThrowsAsync<InvalidOperationException>(() => base.Can_change_state_from_Deleted_with_complex_field_collection(newState, async));

    public override Task Can_change_state_from_Deleted_with_complex_field_record_collection(EntityState newState, bool async)
        => Assert.ThrowsAsync<InvalidOperationException>(() => base.Can_change_state_from_Deleted_with_complex_field_record_collection(newState, async));

    public override Task Can_change_state_from_Deleted_with_complex_record_collection(EntityState newState, bool async)
        => Assert.ThrowsAsync<InvalidOperationException>(() => base.Can_change_state_from_Deleted_with_complex_record_collection(newState, async));

    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
        => facade.UseTransaction(transaction.GetDbTransaction());

    public class SnowflakeFixture : RelationalFixtureBase, ITestSqlLoggerFactory
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}


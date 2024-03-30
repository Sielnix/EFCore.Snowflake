using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestModels.ConferencePlanner;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;
public class ConferencePlannerSnowflakeTest : ConferencePlannerTestBase<ConferencePlannerSnowflakeTest.ConferencePlannerSnowflakeFixture>
{
    public ConferencePlannerSnowflakeTest(ConferencePlannerSnowflakeFixture fixture)
        : base(fixture)
    {
    }

    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
        => facade.UseTransaction(transaction.GetDbTransaction());

    public class ConferencePlannerSnowflakeFixture : ConferencePlannerFixtureBase
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            // todo: move this test to verify against Hybrid tables (they have indexes)
            // after that remove this method overload
            base.OnModelCreating(modelBuilder, context);

            IMutableIndex index = modelBuilder.Entity<Attendee>()
                .HasIndex(u => u.UserName).Metadata;

            IMutableIndex? applicationUserType = modelBuilder.Entity<Attendee>().Metadata.RemoveIndex(index.Properties);
        }

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}


using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.TestModels.ManyToManyFieldsModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class ManyToManyFieldsLoadSnowflakeTest : ManyToManyFieldsLoadTestBase<
    ManyToManyFieldsLoadSnowflakeTest.ManyToManyFieldsLoadSnowflakeFixture>
{
    public ManyToManyFieldsLoadSnowflakeTest(ManyToManyFieldsLoadSnowflakeFixture fixture)
        : base(fixture)
    {
    }

    public override async Task Load_collection_already_loaded(EntityState state, bool async)
    {
        if (state == EntityState.Deleted)
        {
            // todo: use hybrid tables
            return;
        }

        await base.Load_collection_already_loaded(state, async);
    }

    public override async Task Load_collection_already_loaded_untyped(
        EntityState state,
        bool async,
        CascadeTiming deleteOrphansTiming)
    {
        if (state == EntityState.Deleted)
        {
            // todo: use hybrid tables
            return;
        }

        await base.Load_collection_already_loaded_untyped(state, async, deleteOrphansTiming);
    }

    public override async Task Load_collection_using_Query_already_loaded(EntityState state, bool async)
    {
        if (state == EntityState.Deleted)
        {
            // todo: use hybrid tables
            return;
        }

        await base.Load_collection_using_Query_already_loaded(state, async);
    }

    public override  async Task Load_collection_using_Query_already_loaded_untyped(
        EntityState state,
        bool async,
        CascadeTiming deleteOrphansTiming)
    {
        if (state == EntityState.Deleted)
        {
            // todo: use hybrid tables
            return;
        }

        await base.Load_collection_using_Query_already_loaded_untyped(state, async, deleteOrphansTiming);
    }

    public class ManyToManyFieldsLoadSnowflakeFixture : ManyToManyFieldsLoadFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            modelBuilder
                .Entity<JoinOneSelfPayload>()
                .Property(e => e.Payload)
                .HasDefaultValueSql("SYSTIMESTAMP()");

            modelBuilder
                .SharedTypeEntity<Dictionary<string, object>>("JoinOneToThreePayloadFullShared")
                .IndexerProperty<string>("Payload")
                .HasDefaultValue("Generated");

            modelBuilder
                .Entity<JoinOneToThreePayloadFull>()
                .Property(e => e.Payload)
                .HasDefaultValue("Generated");
        }
    }
}

using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.TestModels.ManyToManyModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class ManyToManyLoadSnowflakeTest : ManyToManyLoadTestBase<ManyToManyLoadSnowflakeTest.ManyToManyLoadSnowflakeFixture>
{
    public ManyToManyLoadSnowflakeTest(ManyToManyLoadSnowflakeFixture fixture)
        : base(fixture)
    {
    }

    public override async Task Load_collection_already_loaded(EntityState state, bool async)
    {
        if (state is EntityState.Deleted or EntityState.Detached)
        {
            // todo: hybrid tables
            return;
        }

        await base.Load_collection_already_loaded(state, async);
    }

    public override async Task Load_collection_already_loaded_unidirectional(EntityState state, bool async)
    {
        if (state is EntityState.Deleted)
        {
            // todo: hybrid tables
            return;
        }

        await base.Load_collection_already_loaded_unidirectional(state, async);
    }

    public override async Task Load_collection_already_loaded_untyped(
        EntityState state,
        bool async,
        CascadeTiming deleteOrphansTiming)
    {
        if (state is EntityState.Deleted or EntityState.Detached)
        {
            // todo: hybrid tables
            return;
        }

        await base.Load_collection_already_loaded_untyped(state, async, deleteOrphansTiming);
    }

    public override async Task Load_collection_already_loaded_untyped_unidirectional(
        EntityState state,
        bool async,
        CascadeTiming deleteOrphansTiming)
    {
        if (state is EntityState.Deleted)
        {
            // todo: hybrid tables
            return;
        }

        await base.Load_collection_already_loaded_untyped_unidirectional(state, async, deleteOrphansTiming);
    }

    public override async Task Load_collection_partially_loaded(EntityState state, bool forceIdentityResolution, bool async)
    {
        if (state is EntityState.Deleted or EntityState.Detached)
        {
            // todo: hybrid tables
            return;
        }

        await base.Load_collection_partially_loaded(state, forceIdentityResolution, async);
    }

    public override async Task Load_collection_partially_loaded_no_explicit_join(
        EntityState state,
        bool forceIdentityResolution,
        bool async)
    {
        if (state is EntityState.Deleted or EntityState.Detached)
        {
            // todo: hybrid tables
            return;
        }

        await base.Load_collection_partially_loaded_no_explicit_join(state, forceIdentityResolution, async);
    }

    public override async Task Load_collection_using_Query_already_loaded(EntityState state, bool async)
    {
        if (state is EntityState.Deleted or EntityState.Detached)
        {
            // todo: hybrid tables
            return;
        }

        await base.Load_collection_using_Query_already_loaded(state, async);
    }

    public override async Task Load_collection_using_Query_already_loaded_unidirectional(EntityState state, bool async)
    {
        if (state is EntityState.Deleted)
        {
            // todo: hybrid tables
            return;
        }

        await base.Load_collection_using_Query_already_loaded_unidirectional(state, async);
    }

    public override async Task Load_collection_using_Query_already_loaded_untyped(
        EntityState state,
        bool async,
        CascadeTiming deleteOrphansTiming)
    {
        if (state is EntityState.Deleted or EntityState.Detached)
        {
            // todo: hybrid tables
            return;
        }

        await base.Load_collection_using_Query_already_loaded_untyped(state, async, deleteOrphansTiming);
    }

    public override async Task Load_collection_using_Query_already_loaded_untyped_unidirectional(
        EntityState state,
        bool async,
        CascadeTiming deleteOrphansTiming)
    {
        if (state is EntityState.Deleted)
        {
            // todo: hybrid tables
            return;
        }

        await base.Load_collection_using_Query_already_loaded_untyped_unidirectional(state, async, deleteOrphansTiming);
    }

    public class ManyToManyLoadSnowflakeFixture : ManyToManyLoadFixtureBase
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

            modelBuilder
                .Entity<UnidirectionalJoinOneSelfPayload>()
                .Property(e => e.Payload)
                .HasDefaultValueSql("SYSTIMESTAMP()");

            modelBuilder
                .SharedTypeEntity<Dictionary<string, object>>("UnidirectionalJoinOneToThreePayloadFullShared")
                .IndexerProperty<string>("Payload")
                .HasDefaultValue("Generated");

            modelBuilder
                .Entity<UnidirectionalJoinOneToThreePayloadFull>()
                .Property(e => e.Payload)
                .HasDefaultValue("Generated");
        }
    }
}

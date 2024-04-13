using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestModels.ManyToManyModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class ManyToManyTrackingSnowflakeTest(ManyToManyTrackingSnowflakeTest.ManyToManyTrackingSnowflakeFixture fixture)
    : ManyToManyTrackingRelationalTestBase<ManyToManyTrackingSnowflakeTest.ManyToManyTrackingSnowflakeFixture>(fixture)
{
    protected override Dictionary<string, DeleteBehavior> CustomDeleteBehaviors { get; } = new()
    {
        { "EntityBranch.RootSkipShared", DeleteBehavior.NoAction },
        { "EntityBranch2.Leaf2SkipShared", DeleteBehavior.NoAction },
        { "EntityBranch2.SelfSkipSharedLeft", DeleteBehavior.NoAction },
        { "EntityOne.SelfSkipPayloadLeft", DeleteBehavior.NoAction },
        { "EntityTableSharing1.TableSharing2Shared", DeleteBehavior.NoAction },
        { "EntityTwo.SelfSkipSharedLeft", DeleteBehavior.NoAction },
        { "UnidirectionalEntityBranch.UnidirectionalEntityRoot", DeleteBehavior.NoAction },
        { "UnidirectionalEntityOne.SelfSkipPayloadLeft", DeleteBehavior.NoAction },
        { "UnidirectionalEntityTwo.SelfSkipSharedRight", DeleteBehavior.NoAction },
    };

    [ConditionalFact(Skip="Use hybrid")]
#pragma warning disable xUnit1024 // Test methods cannot have overloads
    public new void Many_to_many_delete_behaviors_are_set()
    {
        base.Many_to_many_delete_behaviors_are_set();
    }
#pragma warning restore xUnit1024 // Test methods cannot have overloads

    [ConditionalFact(Skip = "Use Hybrid")]
    public override void Can_delete_with_many_to_many()
    {
        base.Can_delete_with_many_to_many();
    }

    [ConditionalFact(Skip = "Use Hybrid")]
    public override void Can_delete_with_many_to_many_composite_additional_pk_with_navs()
    {
        base.Can_delete_with_many_to_many_composite_additional_pk_with_navs();
    }

    [ConditionalFact(Skip = "Use Hybrid")]
    public override void Can_delete_with_many_to_many_composite_additional_pk_with_navs_unidirectional()
    {
        base.Can_delete_with_many_to_many_composite_additional_pk_with_navs_unidirectional();
    }

    [ConditionalFact(Skip = "Use Hybrid")]
    public override void Can_delete_with_many_to_many_composite_shared_with_navs()
    {
        base.Can_delete_with_many_to_many_composite_shared_with_navs();
    }

    [ConditionalFact(Skip = "Use Hybrid")]
    public override void Can_delete_with_many_to_many_composite_with_navs()
    {
        base.Can_delete_with_many_to_many_composite_with_navs();
    }

    [ConditionalFact(Skip = "Use Hybrid")]
    public override void Can_delete_with_many_to_many_composite_with_navs_unidirectional()
    {
        base.Can_delete_with_many_to_many_composite_with_navs_unidirectional();
    }

    [ConditionalFact(Skip = "Use Hybrid")]
    public override void Can_delete_with_many_to_many_with_navs()
    {
        base.Can_delete_with_many_to_many_with_navs();
    }

    [ConditionalTheory(Skip = "Use Hybrid tables")]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public override async Task Can_replace_dependent_with_many_to_many(bool createNewCollection, bool async)
    {
        await base.Can_replace_dependent_with_many_to_many(createNewCollection, async);
    }

    public class ManyToManyTrackingSnowflakeFixture : ManyToManyTrackingRelationalFixture//, ITestSqlLoggerFactory
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

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

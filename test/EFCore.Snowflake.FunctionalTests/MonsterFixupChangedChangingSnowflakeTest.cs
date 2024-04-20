using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class MonsterFixupChangedChangingSnowflakeTest :
    MonsterFixupTestBase<MonsterFixupChangedChangingSnowflakeTest.MonsterFixupChangedChangingSnowflakeFixture>
{
    public MonsterFixupChangedChangingSnowflakeTest(MonsterFixupChangedChangingSnowflakeFixture fixture)
        : base(fixture)
    {
    }

    public class MonsterFixupChangedChangingSnowflakeFixture : MonsterFixupChangedChangingFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        protected override void OnModelCreating<TMessage, TProduct, TProductPhoto, TProductReview, TComputerDetail, TDimensions>(
            ModelBuilder builder)
        {
            base.OnModelCreating<TMessage, TProduct, TProductPhoto, TProductReview, TComputerDetail, TDimensions>(builder);

            builder.Entity<TMessage>().Property(e => e.MessageId).UseIdentityColumn();
            builder.Entity<TProductPhoto>().Property(e => e.PhotoId).UseIdentityColumn();
            builder.Entity<TProductReview>().Property(e => e.ReviewId).UseIdentityColumn();
        }
    }
}

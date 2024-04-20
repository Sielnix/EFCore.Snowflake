using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;
public class MusicStoreSnowflakeTest(MusicStoreSnowflakeTest.MusicStoreSnowflakeFixture fixture)
    : MusicStoreTestBase<MusicStoreSnowflakeTest.MusicStoreSnowflakeFixture>(fixture)
{
    public class MusicStoreSnowflakeFixture : MusicStoreFixtureBase
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            // TODO: duplicate this test for hybrid tables with indexes
            foreach (var entity in modelBuilder.Model.GetEntityTypes())
            {
                foreach (var index in entity.GetIndexes().ToList())
                {
                    entity.RemoveIndex(index);
                }
            }
        }

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

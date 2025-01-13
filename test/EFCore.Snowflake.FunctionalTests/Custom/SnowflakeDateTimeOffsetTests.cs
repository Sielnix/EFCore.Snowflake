using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Custom;

public class SnowflakeDateTimeOffsetTests(SnowflakeDateTimeOffsetTests.SnowflakeDateTimeOffsetFixture fixture)
    : IClassFixture<SnowflakeDateTimeOffsetTests.SnowflakeDateTimeOffsetFixture>
{
    private static readonly DateTime LargeDateTime = new(9999, 12, 30, 23, 0, 0, DateTimeKind.Utc);

    // https://github.com/Sielnix/EFCore.Snowflake/issues/12
    [ConditionalFact]
    public virtual async Task Accepts_wide_range_values()
    {
        await using DbContext context = CreateContext();

        Item item = await context.Set<Item>().SingleAsync();

        Assert.Equal(LargeDateTime, item.TzOffset.DateTime);
        Assert.Equal(TimeSpan.Zero, item.TzOffset.Offset);
    }

    protected DbContext CreateContext() => fixture.CreateContext();

    protected class Item
    {
        public long Id { get; set; }
        public DateTimeOffset TzOffset { get; set; }
    }

    public class SnowflakeDateTimeOffsetFixture : SharedStoreFixtureBase<PoolableDbContext>
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            modelBuilder.Entity<Item>()
                .Property(p => p.TzOffset)
                .HasColumnType("TIMESTAMP_TZ(9)");
        }

        protected override async Task SeedAsync(PoolableDbContext context)
        {
            context.AddRange(
                new Item()
                {
                    Id = 123,
                    TzOffset = new DateTimeOffset(LargeDateTime, TimeSpan.Zero)
                });

            await context.SaveChangesAsync();
        }

        protected override string StoreName => "SnowflakeDateTimeOffset";
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

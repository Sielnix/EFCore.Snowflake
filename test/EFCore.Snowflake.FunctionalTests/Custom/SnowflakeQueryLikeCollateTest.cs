using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Custom;

public class SnowflakeQueryLikeCollateTest : IClassFixture<SnowflakeQueryLikeCollateTest.SnowflakeQueryLikeCollateFixture>
{
    public SnowflakeQueryLikeCollateTest(SnowflakeQueryLikeCollateFixture fixture)
    {
        Fixture = fixture;
    }

    protected SnowflakeQueryLikeCollateFixture Fixture { get; }

    [ConditionalFact]
    public virtual async Task Queries_like_collate()
    {
        await using DbContext context = CreateContext();

        const string searchPattern = $"%ö%";


        List<Item> items = await context.Set<Item>()
            .Where(r => EF.Functions.Like(EF.Functions.Collate(r.SomeText, "de-ci"), searchPattern))
            .ToListAsync();

        Item item = Assert.Single(items);
        Assert.Equal(456, item.Id);
    }

    protected DbContext CreateContext() => Fixture.CreateContext();

    protected class Item
    {
        public long Id { get; set; }
        public string SomeText { get; set; } = null!;
    }

    public class SnowflakeQueryLikeCollateFixture : SharedStoreFixtureBase<PoolableDbContext>
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            modelBuilder.Entity<Item>();
        }

        protected override async Task SeedAsync(PoolableDbContext context)
        {
            context.AddRange(
                new Item()
                {
                    Id = 123,
                    SomeText = "UPPER_TEXT"
                },
                new Item()
                {
                    Id = 456,
                    SomeText = "lower_text_ÖÜẞß"
                });

            await context.SaveChangesAsync();
        }

        protected override string StoreName => "LikeCollateQuery";
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

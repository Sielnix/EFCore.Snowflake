using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Custom;

public class SnowflakeFilterTests : IClassFixture<SnowflakeFilterTests.SnowflakeFilterFixture>
{
    public SnowflakeFilterTests(SnowflakeFilterFixture fixture)
    {
        Fixture = fixture;
    }

    protected SnowflakeFilterFixture Fixture { get; }
    
    [ConditionalFact]
    public virtual void Inserts_TimeOnly()
    {
        using DbContext context = CreateContext();
        TimeOnly toInsert = new(1, 0);

        TimeOnlyFilter f = new()
        {
            Id = 999,
            Time = toInsert
        };

        context.Set<TimeOnlyFilter>().Add(f);
        context.SaveChanges();
        context.ChangeTracker.Clear();

        TimeOnlyFilter result = context.Set<TimeOnlyFilter>().Single(t => t.Id == 999);
        Assert.Equal(result.Time, toInsert);
    }


    [ConditionalFact]
    public virtual void Searches_by_TimeOnly()
    {
        using DbContext context = CreateContext();
        TimeOnly searchFilter = new(13, 0);

        List<TimeOnlyFilter> result = context.Set<TimeOnlyFilter>().Where(s => s.Time >= searchFilter).ToList();

        TimeOnlyFilter singleItem = Assert.Single(result);

        Assert.Equal(456, singleItem.Id);
    }


    [ConditionalFact]
    public virtual void Filter_by_TimeOnly()
    {
        using DbContext context = CreateContext();
        TimeOnly searchFilter = new(13, 30);

        List<TimeOnlyFilter> result = context.Set<TimeOnlyFilter>().Where(s => s.Time == searchFilter).ToList();

        TimeOnlyFilter singleItem = Assert.Single(result);

        Assert.Equal(456, singleItem.Id);
    }

    protected DbContext CreateContext() => Fixture.CreateContext();

    protected class TimeOnlyFilter
    {
        public long Id { get; set; }
        public TimeOnly Time { get; set; }
    }

    public class SnowflakeFilterFixture : SharedStoreFixtureBase<PoolableDbContext>
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            modelBuilder.Entity<TimeOnlyFilter>();
        }



        protected override async Task SeedAsync(PoolableDbContext context)
        {
            context.AddRange(
                new TimeOnlyFilter()
                {
                    Id = 123,
                    Time = new(12, 20)
                },
                new TimeOnlyFilter()
                {
                    Id = 456,
                    Time = new(13, 30)
                });

            await context.SaveChangesAsync();
        }

        protected override string StoreName => "Filter";
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Custom;

/// <summary>
/// https://github.com/Sielnix/EFCore.Snowflake/issues/22
/// </summary>
public class SnowflakeTrimTests(SnowflakeTrimTests.SnowflakeTrimFixture fixture)
    : IClassFixture<SnowflakeTrimTests.SnowflakeTrimFixture>
{
    [ConditionalFact]
    public virtual async Task Trims_with_no_args()
    {
        await using DbContext context = CreateContext();
        IQueryable<string> stringQuery = context.Set<Item>()
            .Where(it => it.Id == 1)
            .Select(it => it.Value);

        string trim = await stringQuery.Select(q => q.Trim()).SingleAsync();
        Assert.Equal("val", trim);
        
        string trimLeft = await stringQuery.Select(q => q.TrimStart()).SingleAsync();
        Assert.Equal("val  ", trimLeft);
        
        string trimRight = await stringQuery.Select(q => q.TrimEnd()).SingleAsync();
        Assert.Equal("  val", trimRight);
    }

    [ConditionalFact]
    public virtual async Task Trims_with_single_char_arg()
    {
        await using DbContext context = CreateContext();
        IQueryable<string> stringQuery = context.Set<Item>()
            .Where(it => it.Id == 2)
            .Select(it => it.Value);

        string trim = await stringQuery.Select(q => q.Trim('a')).SingleAsync();
        Assert.Equal("babXDbab", trim);

        string trimLeft = await stringQuery.Select(q => q.TrimStart('a')).SingleAsync();
        Assert.Equal("babXDbabaa", trimLeft);

        string trimRight = await stringQuery.Select(q => q.TrimEnd('a')).SingleAsync();
        Assert.Equal("ababXDbab", trimRight);
    }

    [ConditionalFact]
    public virtual async Task Trims_with_multiple_char_args()
    {
        await using DbContext context = CreateContext();
        IQueryable<string> stringQuery = context.Set<Item>()
            .Where(it => it.Id == 2)
            .Select(it => it.Value);

        string trim = await stringQuery.Select(q => q.Trim('a', 'b')).SingleAsync();
        Assert.Equal("XD", trim);

        string trimLeft = await stringQuery.Select(q => q.TrimStart('a', 'b')).SingleAsync();
        Assert.Equal("XDbabaa", trimLeft);

        string trimRight = await stringQuery.Select(q => q.TrimEnd('a', 'b')).SingleAsync();
        Assert.Equal("ababXD", trimRight);
    }


    protected DbContext CreateContext() => fixture.CreateContext();

    protected class Item
    {
        public long Id { get; set; }
        public string Value { get; set; } = null!;
    }
    
    public class SnowflakeTrimFixture : SharedStoreFixtureBase<PoolableDbContext>
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            modelBuilder.Entity<Item>()
                .Property(p => p.Value)
                .HasMaxLength(100);
        }

        protected override async Task SeedAsync(PoolableDbContext context)
        {
            context.AddRange(new Item() { Id = 1, Value = "  val  " });
            context.AddRange(new Item() { Id = 2, Value = "ababXDbabaa" });
            

            await context.SaveChangesAsync();
        }

        protected override string StoreName => "SnowflakeDateTimeOffset";
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

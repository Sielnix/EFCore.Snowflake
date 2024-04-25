using System.ComponentModel.DataAnnotations.Schema;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Custom;

public class SnowflakeIndirectTypesTests(SnowflakeIndirectTypesTests.SnowflakeBoolAsIntFixture fixture)
    : IClassFixture<SnowflakeIndirectTypesTests.SnowflakeBoolAsIntFixture>
{
    protected SnowflakeBoolAsIntFixture Fixture { get; } = fixture;

    [ConditionalFact]
    public virtual void Inserts_and_reads_values()
    {
        using DbContext context = CreateContext();

        HardTypes item1 = context.Set<HardTypes>().Single(t => t.Id == 123);
        Assert.True(item1.BoolValCol);
        Assert.Equal(12.34, item1.DoubleAsNumber);
        
        HardTypes item2 = context.Set<HardTypes>().Single(t => t.Id == 456);
        Assert.False(item2.BoolValCol);
        Assert.Equal(5, item2.DoubleAsNumber);

        HardTypes item3 = context.Set<HardTypes>().Single(t => t.Id == 789);
        Assert.Null(item3.BoolValCol);
        Assert.Null(item3.DoubleAsNumber);
    }

    protected DbContext CreateContext() => Fixture.CreateContext();

    protected class HardTypes
    {
        public long Id { get; set; }

        [Column(TypeName = "int")]
        public bool? BoolValCol { get; set; }

        public double? DoubleAsNumber { get; set; }
    }

    public class SnowflakeBoolAsIntFixture : SharedStoreFixtureBase<PoolableDbContext>
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            EntityTypeBuilder<HardTypes> entConf = modelBuilder.Entity<HardTypes>();

            entConf.Property(e => e.DoubleAsNumber)
                .HasPrecision(38, 2)
                .HasColumnType("NUMERIC");
        }

        protected override void Seed(PoolableDbContext context)
        {
            context.AddRange(
                new HardTypes()
                {
                    Id = 123,
                    BoolValCol = true,
                    DoubleAsNumber = 12.344
                },
                new HardTypes()
                {
                    Id = 456,
                    BoolValCol = false,
                    DoubleAsNumber = 5
                },
                new HardTypes()
                {
                    Id = 789,
                    BoolValCol = null,
                    DoubleAsNumber = null
                });

            context.SaveChanges();
        }

        protected override string StoreName => "IndirectTypes";
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

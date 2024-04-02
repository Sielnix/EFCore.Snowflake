using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Snowflake.FunctionalTests;

public class DefaultValuesTest : IDisposable
{
    private readonly IServiceProvider _serviceProvider = new ServiceCollection()
        .AddEntityFrameworkSnowflake()
        .BuildServiceProvider(validateScopes: true);

    public DefaultValuesTest()
    {
        TestStore = SnowflakeTestStore.CreateInitialized("DefaultValuesTest");
    }

    protected SnowflakeTestStore TestStore { get; }

    public virtual void Dispose()
        => TestStore.Dispose();

    [ConditionalFact]
    public void Can_use_Snowflake_default_values()
    {
        DateTime defaultDate = new DateTime(2035, 9, 25);
        DateTime customDate = new DateTime(2111, 1, 11);

        DateTimeOffset defaultDateOffset = new DateTimeOffset(defaultDate);
        DateTimeOffset customDateOffset = new DateTimeOffset(customDate);
        
        using (var context = new ChipsContext(_serviceProvider, TestStore.Name))
        {
            context.Database.EnsureCreatedResiliently();

            context.Chippers.Add(
                new Chipper { Id = "Default" });

            context.SaveChanges();

            var honeyDijon = context.Add(
                new KettleChips { Name = "Honey Dijon" }).Entity;
            var buffaloBleu = context.Add(
                new KettleChips { Name = "Buffalo Bleu", BestBuyDate = customDate, BestBuyDateOffset = customDateOffset}).Entity;

            context.SaveChanges();

            Assert.Equal(defaultDate, honeyDijon.BestBuyDate);
            Assert.Equal(customDate, buffaloBleu.BestBuyDate);

            Assert.Equal(defaultDateOffset, honeyDijon.BestBuyDateOffset);
            Assert.Equal(customDateOffset, buffaloBleu.BestBuyDateOffset);
        }

        using (var context = new ChipsContext(_serviceProvider, TestStore.Name))
        {
            KettleChips fetchedDefault = context.Chips.Single(c => c.Name == "Honey Dijon");
            KettleChips fetchedCustom = context.Chips.Single(c => c.Name == "Buffalo Bleu");

            Assert.Equal(defaultDate, fetchedDefault.BestBuyDate);
            Assert.Equal(customDate, fetchedCustom.BestBuyDate);

            Assert.Equal(defaultDateOffset, fetchedDefault.BestBuyDateOffset);
            Assert.Equal(customDateOffset, fetchedCustom.BestBuyDateOffset);
        }
    }

    private class ChipsContext : DbContext
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly string _databaseName;

        public ChipsContext(IServiceProvider serviceProvider, string databaseName)
        {
            _serviceProvider = serviceProvider;
            _databaseName = databaseName;
        }

        public DbSet<KettleChips> Chips { get; set; } = null!;
        public DbSet<Chipper> Chippers { get; set; } = null!;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder
                .UseSnowflake(SnowflakeTestStore.CreateConnectionString(_databaseName), b => b.ApplyConfiguration())
                .UseInternalServiceProvider(_serviceProvider);

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<KettleChips>(
                b =>
                {
                    b.Property(e => e.BestBuyDate)
                        .ValueGeneratedOnAdd()
                        .HasDefaultValue(new DateTime(2035, 9, 25));

                    b.Property(e => e.BestBuyDateOffset)
                        .ValueGeneratedOnAdd()
                        .HasDefaultValue(new DateTimeOffset(new DateTime(2035, 9, 25)));

                    b.Property(e => e.ChipperId)
                        .IsRequired()
                        .HasDefaultValue("Default");
                });
    }

    private class KettleChips
    {
        public int Id { get; set; }
        public string Name { get; set; } = null!;
        public DateTime BestBuyDate { get; set; }
        public DateTimeOffset BestBuyDateOffset { get; set; }
        public string ChipperId { get; set; } = null!;

        public Chipper Manufacturer { get; set; } = null!;
    }

    private class Chipper
    {
        public string Id { get; set; } = null!;
    }
}

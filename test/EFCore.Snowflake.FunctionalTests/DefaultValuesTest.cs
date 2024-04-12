using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Snowflake.FunctionalTests;

public class DefaultValuesTest : IDisposable
{
    private const string DefaultDescription = "Best Chips";
    private const string CustomDescription = "Probably Best Chips";

    private const long DefaultAmount = 150;
    private const long CustomAmount = 200;

    private const decimal DefaultPrice = 12.5m;
    private const decimal CustomPrice = 15.8m;

    private const double DefaultSize = 8.8;
    private const double CustomSize = 9.9;

    private static readonly DateTime DefaultDate = new(2035, 9, 25);
    private static readonly DateTime CustomDate = new(2111, 1, 11);
    
    private static readonly DateTimeOffset DefaultDateOffset = new(DefaultDate);
    private static readonly DateTimeOffset CustomDateOffset = new(CustomDate);

    private static readonly int[] DefaultValues = [1, 2, 3];
    private static readonly int[] CustomValues = [4, 5];

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
        using (var context = new ChipsContext(_serviceProvider, TestStore.Name))
        {
            context.Database.EnsureCreatedResiliently();

            context.Chippers.Add(
                new Chipper { Id = "Default" });

            context.SaveChanges();

            var honeyDijon = context.Add(
                new KettleChips { Name = "Honey Dijon" }).Entity;
            var buffaloBleu = context.Add(
                new KettleChips
                {
                    Name = "Buffalo Bleu",
                    BestBuyDate = CustomDate,
                    BestBuyDateOffset = CustomDateOffset,
                    Amount = CustomAmount,
                    Price = CustomPrice,
                    Size = CustomSize,
                    Description = CustomDescription,
                    Values = CustomValues
                }).Entity;

            context.SaveChanges();

            Assert.Equal(DefaultDate, honeyDijon.BestBuyDate);
            Assert.Equal(DefaultDateOffset, honeyDijon.BestBuyDateOffset);
            Assert.Equal(DefaultAmount, honeyDijon.Amount);
            Assert.Equal(DefaultPrice, honeyDijon.Price);
            Assert.Equal(DefaultSize, honeyDijon.Size);
            Assert.Equal(DefaultDescription, honeyDijon.Description);
            Assert.Equal(DefaultValues, honeyDijon.Values);

            Assert.Equal(CustomDate, buffaloBleu.BestBuyDate);
            Assert.Equal(CustomDateOffset, buffaloBleu.BestBuyDateOffset);
            Assert.Equal(CustomAmount, buffaloBleu.Amount);
            Assert.Equal(CustomPrice, buffaloBleu.Price);
            Assert.Equal(CustomSize, buffaloBleu.Size);
            Assert.Equal(CustomDescription, buffaloBleu.Description);
            Assert.Equal(CustomValues, buffaloBleu.Values);
        }

        using (var context = new ChipsContext(_serviceProvider, TestStore.Name))
        {
            KettleChips fetchedDefault = context.Chips.Single(c => c.Name == "Honey Dijon");
            KettleChips fetchedCustom = context.Chips.Single(c => c.Name == "Buffalo Bleu");

            Assert.Equal(DefaultDate, fetchedDefault.BestBuyDate);
            Assert.Equal(DefaultDateOffset, fetchedDefault.BestBuyDateOffset);
            Assert.Equal(DefaultAmount, fetchedDefault.Amount);
            Assert.Equal(DefaultPrice, fetchedDefault.Price);
            Assert.Equal(DefaultSize, fetchedDefault.Size);
            Assert.Equal(DefaultDescription, fetchedDefault.Description);
            Assert.Equal(DefaultValues, fetchedDefault.Values);

            Assert.Equal(CustomDate, fetchedCustom.BestBuyDate);
            Assert.Equal(CustomDateOffset, fetchedCustom.BestBuyDateOffset);
            Assert.Equal(CustomAmount, fetchedCustom.Amount);
            Assert.Equal(CustomPrice, fetchedCustom.Price);
            Assert.Equal(CustomSize, fetchedCustom.Size);
            Assert.Equal(CustomDescription, fetchedCustom.Description);
            Assert.Equal(CustomValues, fetchedCustom.Values);
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
                        .HasDefaultValue(DefaultDate);

                    b.Property(e => e.BestBuyDateOffset)
                        .ValueGeneratedOnAdd()
                        .HasDefaultValue(DefaultDateOffset);

                    b.Property(e => e.Amount)
                        .ValueGeneratedOnAdd()
                        .HasDefaultValue(DefaultAmount);

                    b.Property(e => e.Price)
                        .ValueGeneratedOnAdd()
                        .HasDefaultValue(DefaultPrice);

                    b.Property(e => e.Size)
                        .ValueGeneratedOnAdd()
                        .HasDefaultValue(DefaultSize);

                    b.Property(e => e.ChipperId)
                        .IsRequired()
                        .HasDefaultValue("Default");

                    b.Property(e => e.Description)
                        .IsRequired()
                        .HasDefaultValue(DefaultDescription);

                    b.Property(e => e.Values)
                        .ValueGeneratedOnAdd()
                        .HasDefaultValue(DefaultValues);
                });
    }

    private class KettleChips
    {
        public int Id { get; set; }
        public string Name { get; set; } = null!;
        public DateTime BestBuyDate { get; set; }
        public DateTimeOffset BestBuyDateOffset { get; set; }
        public long Amount { get; set; }
        public decimal Price { get; set; }
        public double Size { get; set; }
        public string Description { get; set; } = null!;
        public string ChipperId { get; set; } = null!;
        public int[]? Values { get; set; }

        public Chipper Manufacturer { get; set; } = null!;
    }

    private class Chipper
    {
        public string Id { get; set; } = null!;
    }
}

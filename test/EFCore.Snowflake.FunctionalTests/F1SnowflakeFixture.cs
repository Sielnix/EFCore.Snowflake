using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.EntityFrameworkCore.TestModels.ConcurrencyModel;
using Microsoft.EntityFrameworkCore.TestUtilities;
using System.ComponentModel.DataAnnotations;

namespace EFCore.Snowflake.FunctionalTests;

public class F1ULongSnowflakeFixture : F1SnowflakeFixtureBase<ulong>
{
    protected override string StoreName
        => "F1TestULong";

    protected override void BuildModelExternal(ModelBuilder modelBuilder)
    {
        base.BuildModelExternal(modelBuilder);

        modelBuilder.Entity<Chassis>().Property<ulong>("Version").HasConversion<byte[]>();
        modelBuilder.Entity<Driver>().Property<ulong>("Version").HasConversion<byte[]>();
        modelBuilder.Entity<Team>().Property<ulong>("Version").HasConversion<byte[]>();
        modelBuilder.Entity<Sponsor>().Property<ulong>("Version").HasConversion<byte[]>();
        modelBuilder.Entity<TitleSponsor>()
            .OwnsOne(
                s => s.Details, eb =>
                {
                    eb.Property<ulong>("Version").IsRowVersion();
                });

        modelBuilder.Entity<OptimisticOptionalChild>();

        modelBuilder
            .Entity<OptimisticParent>()
            .HasData(
                new OptimisticParent { Id = new Guid("AF8451C3-61CB-4EDA-8282-92250D85EF03"), }
            );

        modelBuilder
            .Entity<Fan>()
            .Property(e => e.ULongVersion)
            .IsRowVersion()
            .HasConversion<byte[]>();

        modelBuilder
            .Entity<FanTpt>()
            .Property(e => e.ULongVersion)
            .IsRowVersion()
            .HasConversion<byte[]>();

        modelBuilder
            .Entity<FanTpc>()
            .Property(e => e.ULongVersion)
            .IsRowVersion()
            .HasConversion<byte[]>();

        modelBuilder
            .Entity<Circuit>()
            .Property(e => e.ULongVersion)
            .IsRowVersion()
            .HasConversion<byte[]>();

        modelBuilder
            .Entity<CircuitTpt>()
            .Property(e => e.ULongVersion)
            .IsRowVersion()
            .HasConversion<byte[]>();

        modelBuilder
            .Entity<CircuitTpc>()
            .Property(e => e.ULongVersion)
            .IsRowVersion()
            .HasConversion<byte[]>();
    }

    public class OptimisticOptionalChild
    {
        public Guid Id { get; set; }
        public ICollection<OptimisticParent> Parents { get; set; } = null!;

        [Timestamp]
        public long Version { get; set; }
    }

    public class OptimisticParent
    {
        public Guid Id { get; set; }
        public OptimisticOptionalChild OptionalChild { get; set; } = null!;
    }
}

public class F1SnowflakeFixture : F1SnowflakeFixtureBase<byte[]>
{
    //protected override bool UsePooling => false;

    protected override void BuildModelExternal(ModelBuilder modelBuilder)
    {
        base.BuildModelExternal(modelBuilder);

        var converter = new BinaryVersionConverter();
        var comparer = new BinaryVersionComparer();

        modelBuilder
            .Entity<Fan>()
            .Property(e => e.BinaryVersion)
            .HasConversion(converter, comparer)
            .IsRowVersion();

        modelBuilder
            .Entity<FanTpt>()
            .Property(e => e.BinaryVersion)
            .HasConversion(converter, comparer)
            .IsRowVersion();

        modelBuilder
            .Entity<FanTpc>()
            .Property(e => e.BinaryVersion)
            .HasConversion(converter, comparer)
            .IsRowVersion();

        modelBuilder
            .Entity<Circuit>()
            .Property(e => e.BinaryVersion)
            .HasConversion(converter, comparer)
            .IsRowVersion();

        modelBuilder
            .Entity<CircuitTpt>()
            .Property(e => e.BinaryVersion)
            .HasConversion(converter, comparer)
            .IsRowVersion();

        modelBuilder
            .Entity<CircuitTpc>()
            .Property(e => e.BinaryVersion)
            .HasConversion(converter, comparer)
            .IsRowVersion();
    }

    private class BinaryVersionConverter : ValueConverter<List<byte>, byte[]?>
    {
        public BinaryVersionConverter()
            : base(
                v => v == null ? null : v.ToArray(),
                v => v == null ? null! : v.ToList())
        {
        }
    }

    private class BinaryVersionComparer : ValueComparer<List<byte>?>
    {
        public BinaryVersionComparer()
            : base(
                (l, r) => (l == null && r == null) || (l != null && r != null && l.SequenceEqual(r)),
                v => CalculateHashCode(v),
                v => v == null ? null : v.ToList())
        {
        }

        private static int CalculateHashCode(List<byte>? source)
        {
            if (source == null)
            {
                return 0;
            }

            var hash = new HashCode();
            foreach (var el in source)
            {
                hash.Add(el);
            }

            return hash.ToHashCode();
        }
    }
}

public abstract class F1SnowflakeFixtureBase<TRowVersion> : F1RelationalFixture<TRowVersion>
{
    protected override ITestStoreFactory TestStoreFactory
        => SnowflakeTestStoreFactory.Instance;

    public override TestHelpers TestHelpers
        => SnowflakeTestHelpers.Instance;

    protected override void BuildModelExternal(ModelBuilder modelBuilder)
    {
        base.BuildModelExternal(modelBuilder);

        modelBuilder.Entity<TitleSponsor>()
            .OwnsOne(
                s => s.Details, eb =>
                {
                    eb.Property(d => d.Space).HasColumnType("decimal(18,2)");
                });
    }
}

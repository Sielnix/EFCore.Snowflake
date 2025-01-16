using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class ArrayQueryTest : IClassFixture<ArrayQueryTest.ArrayQueryFixture>
{
    public ArrayQueryTest(ArrayQueryFixture fixture)
    {
        Fixture = fixture;
    }

    protected ArrayQueryFixture Fixture { get; }

    [ConditionalFact]
    public virtual void Inserts_Reads_Array()
    {
        using ArrayQueryContext context = CreateContext();
        TableItem item = context.TableItems.Single(i => i.Id == 1000);
        Assert.Equal("a", item.StringArray[0]);
        Assert.Equal("B", item.StringArray[1]);
        Assert.Equal("CCCC", item.StringArray[2]);
    }

    [ConditionalFact]
    public virtual void Inserts_Reads_Array_From_Nullable_Column()
    {
        using ArrayQueryContext context = CreateContext();
        TableColumnNullable item = context.TableColumnNullables.Single(i => i.Id == 1);
        Assert.NotNull(item.StringArray);
        Assert.Equal("a", item.StringArray[0]);
        Assert.Equal("B", item.StringArray[1]);
        Assert.Equal("CCCC", item.StringArray[2]);

        TableColumnNullable columnNullable = context.TableColumnNullables.Single(i => i.Id == 2);
        Assert.Null(columnNullable.StringArray);
    }

    [ConditionalFact]
    public virtual void Inserts_Reads_Array_From_Nullable_Items_column()
    {
        using ArrayQueryContext context = CreateContext();
        TableItemNullable item = context.TableItemNullables.Single(i => i.Id == 1);
        Assert.NotNull(item.StringArray);
        Assert.Null(item.StringArray[0]);
        Assert.Equal("a", item.StringArray[1]);
        Assert.Equal("B", item.StringArray[2]);
        Assert.Equal("CCCC", item.StringArray[3]);
        Assert.False(item.BoolArray[0].HasValue);
        Assert.True(item.BoolArray[1]!.Value);
        Assert.False(item.BoolArray[2]!.Value);
    }

    protected ArrayQueryContext CreateContext() => Fixture.CreateContext();

    public class TableItem
    {
        public long Id { get; set; }
        public string[] StringArray { get; set; } = null!;
        public bool[] BoolArray { get; set; } = null!;
        public char[] CharArray { get; set; } = null!;
        public byte[][] ByteArrayArray { get; set; } = null!;
        public sbyte[] SByteArray { get; set; } = null!;
        public short[] ShortArray { get; set; } = null!;
        public ushort[] UShortArray { get; set; } = null!;
        public int[] IntArray { get; set; } = null!;
        public uint[] UIntArray { get; set; } = null!;
        public long[] LongArray { get; set; } = null!;
        public ulong[] ULongArray { get; set; } = null!;
        public decimal[] DecimalArray { get; set; } = null!;
        public double[] DoubleArray { get; set; } = null!;
        public float[] FloatArray { get; set; } = null!;
        public DateOnly[] DateOnlyArray { get; set; } = null!;
        public TimeOnly[] TimeOnlyArray { get; set; } = null!;
        public TimeSpan[] TimeSpanArray { get; set; } = null!;
        public DateTime[] DateTimeArray { get; set; } = null!;
        public DateTimeOffset[] DateTimeOffsetArray { get; set; } = null!;
    }

    public class TableColumnNullable
    {
        public long Id { get; set; }
        public string[]? StringArray { get; set; } = null!;
        public bool[]? BoolArray { get; set; } = null!;
        public char[]? CharArray { get; set; } = null!;
        public byte[][]? ByteArrayArray { get; set; } = null!;
        public sbyte[]? SByteArray { get; set; } = null!;
        public short[]? ShortArray { get; set; } = null!;
        public ushort[]? UShortArray { get; set; } = null!;
        public int[]? IntArray { get; set; } = null!;
        public uint[]? UIntArray { get; set; } = null!;
        public long[]? LongArray { get; set; } = null!;
        public ulong[]? ULongArray { get; set; } = null!;
        public decimal[]? DecimalArray { get; set; } = null!;
        public double[]? DoubleArray { get; set; } = null!;
        public float[]? FloatArray { get; set; } = null!;
        public DateOnly[]? DateOnlyArray { get; set; } = null!;
        public TimeOnly[]? TimeOnlyArray { get; set; } = null!;
        public TimeSpan[]? TimeSpanArray { get; set; } = null!;
        public DateTime[]? DateTimeArray { get; set; } = null!;
        public DateTimeOffset[]? DateTimeOffsetArray { get; set; } = null!;
    }

    public class TableItemNullable
    {
        public long Id { get; set; }
        public string?[] StringArray { get; set; } = null!;
        public bool?[] BoolArray { get; set; } = null!;
        public char?[] CharArray { get; set; } = null!;
        public byte[]?[] ByteArrayArray { get; set; } = null!;
        public sbyte?[] SByteArray { get; set; } = null!;
        public short?[] ShortArray { get; set; } = null!;
        public ushort?[] UShortArray { get; set; } = null!;
        public int?[] IntArray { get; set; } = null!;
        public uint?[] UIntArray { get; set; } = null!;
        public long?[] LongArray { get; set; } = null!;
        public ulong?[] ULongArray { get; set; } = null!;
        public decimal?[] DecimalArray { get; set; } = null!;
        public double?[] DoubleArray { get; set; } = null!;
        public float?[] FloatArray { get; set; } = null!;
        public DateOnly?[] DateOnlyArray { get; set; } = null!;
        public TimeOnly?[] TimeOnlyArray { get; set; } = null!;
        public TimeSpan?[] TimeSpanArray { get; set; } = null!;
        public DateTime?[] DateTimeArray { get; set; } = null!;
        public DateTimeOffset?[] DateTimeOffsetArray { get; set; } = null!;
    }

    public class ArrayQueryContext : PoolableDbContext
    {
        public ArrayQueryContext(DbContextOptions options)
            : base(options)
        {
        }

        public DbSet<TableItem> TableItems { get; set; } = null!;
        public DbSet<TableColumnNullable> TableColumnNullables { get; set; } = null!;
        public DbSet<TableItemNullable> TableItemNullables { get; set; } = null!;
    }

    public class ArrayQueryFixture : SharedStoreFixtureBase<ArrayQueryContext>
    {
        protected override async Task SeedAsync(ArrayQueryContext context)
        {
            DateOnly date = new DateOnly(2024, 3, 29);
            TimeOnly time = new TimeOnly(13, 14, 15, 167);
            DateTime dateTime = new DateTime(date, time);
            TimeSpan timeSpan = TimeSpan.FromMinutes(30);
            
            context.AddRange(new TableItem()
            {
                Id = 1000,
                StringArray = ["a", "B", "CCCC"],
                BoolArray = [true, false],
                ByteArrayArray = [ [0,1], [2, 255] ],
                CharArray = ['a', 'B', '\\'],
                DateOnlyArray = [date],
                DateTimeArray = [default, dateTime],
                DateTimeOffsetArray = [default, new DateTimeOffset(dateTime, timeSpan)],
                DecimalArray = [-10, 1.5m],
                DoubleArray = [-20, 2.5],
                FloatArray = [-30, 3.5f],
                IntArray = [-40, 4],
                LongArray = [-50, 5],
                SByteArray = [60, 0],
                ShortArray = [-70, 7],
                TimeOnlyArray = [default, time],
                TimeSpanArray = [default, timeSpan],
                UIntArray = [80, 8],
                ULongArray = [ulong.MaxValue, 9],
                UShortArray = [ushort.MaxValue, 10],
            },
            new TableColumnNullable()
            {
                Id = 1,
                StringArray = ["a", "B", "CCCC"],
                BoolArray = [true, false],
                ByteArrayArray = [[0, 1], [2, 255]],
                CharArray = ['a', 'B', '\\'],
                DateOnlyArray = [date],
                DateTimeArray = [default, dateTime],
                DateTimeOffsetArray = [default, new DateTimeOffset(dateTime, timeSpan)],
                DecimalArray = [-10, 1.5m],
                DoubleArray = [-20, 2.5],
                FloatArray = [-30, 3.5f],
                IntArray = [-40, 4],
                LongArray = [-50, 5],
                SByteArray = [60, 0],
                ShortArray = [-70, 7],
                TimeOnlyArray = [default, time],
                TimeSpanArray = [default, timeSpan],
                UIntArray = [80, 8],
                ULongArray = [ulong.MaxValue, 9],
                UShortArray = [ushort.MaxValue, 10],
            },
            new TableColumnNullable()
            {
                Id = 2
            },
            new TableItemNullable()
            {
                Id = 1,
                StringArray = [null, "a", "B", "CCCC"],
                BoolArray = [null, true, false],
                ByteArrayArray = [null, [0, 1], [2, 255]],
                CharArray = [null, 'a', 'B', '\\'],
                DateOnlyArray = [null, date],
                DateTimeArray = [null, dateTime],
                DateTimeOffsetArray = [null, new DateTimeOffset(dateTime, timeSpan)],
                DecimalArray = [null, -10, 1.5m],
                DoubleArray = [null, -20, 2.5],
                FloatArray = [null, -30, 3.5f],
                IntArray = [null, -40, 4],
                LongArray = [null, -50, 5],
                SByteArray = [null, 60, 0],
                ShortArray = [null, -70, 7],
                TimeOnlyArray = [null, time],
                TimeSpanArray = [null, timeSpan],
                UIntArray = [null, 80, 8],
                ULongArray = [null, ulong.MaxValue, 9],
                UShortArray = [null, ushort.MaxValue, 10],
            });

            context.AddRange();

            await context.SaveChangesAsync();
        }

        protected override string StoreName => "ArrayQuery";
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

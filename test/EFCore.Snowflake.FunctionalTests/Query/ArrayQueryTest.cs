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

    [ConditionalFact]
    public virtual void Where_Array_Contains_Constant()
    {
        using ArrayQueryContext context = CreateContext();
        TableItem item = context.TableItems.Single(i => i.StringArray.Contains("CCCC"));
        Assert.Equal(1000, item.Id);
    }

    [ConditionalFact]
    public virtual void Where_Array_Contains_Parameter()
    {
        using ArrayQueryContext context = CreateContext();
        string value = "B";
        TableItem item = context.TableItems.Single(i => i.StringArray.Contains(value));
        Assert.Equal(1000, item.Id);
    }

    [ConditionalFact]
    public virtual void Where_Array_Contains_Missing_Value_Returns_No_Results()
    {
        using ArrayQueryContext context = CreateContext();
        bool any = context.TableItems.Any(i => i.StringArray.Contains("does-not-exist"));
        Assert.False(any);
    }

    [ConditionalFact]
    public virtual void Where_Int_Array_Contains_Constant()
    {
        using ArrayQueryContext context = CreateContext();
        TableItem item = context.TableItems.Single(i => i.IntArray.Contains(4));
        Assert.Equal(1000, item.Id);
    }

    [ConditionalFact]
    public virtual void Where_List_Contains_Constant()
    {
        using ArrayQueryContext context = CreateContext();
        TableItem item = context.TableItems.Single(i => i.StringList.Contains("CCCC"));
        Assert.Equal(1000, item.Id);
    }

    [ConditionalFact]
    public virtual void Where_Nullable_Element_Array_Contains_Constant()
    {
        using ArrayQueryContext context = CreateContext();
        TableItemNullable item = context.TableItemNullables.Single(i => i.IntArray.Contains(4));
        Assert.Equal(1, item.Id);
    }

    [ConditionalFact]
    public virtual void Where_Array_Contains_On_Nullable_Column()
    {
        using ArrayQueryContext context = CreateContext();
        TableColumnNullable item = context.TableColumnNullables.Single(i => i.StringArray!.Contains("a"));
        Assert.Equal(1, item.Id);

        bool anyMatchOnNullColumn = context.TableColumnNullables
            .Where(i => i.Id == 2)
            .Any(i => i.StringArray!.Contains("a"));
        Assert.False(anyMatchOnNullColumn);
    }

    [ConditionalFact]
    public virtual void Where_Array_Any_Contains_Constant()
    {
        using ArrayQueryContext context = CreateContext();
        TableItem item = context.TableItems.Single(i => i.StringArray.Any(a => a.Contains("CC")));
        Assert.Equal(1000, item.Id);
    }

    [ConditionalFact]
    public virtual void Where_Array_Any_Contains_Parameter()
    {
        using ArrayQueryContext context = CreateContext();
        string pattern = "CC";
        TableItem item = context.TableItems.Single(i => i.StringArray.Any(a => a.Contains(pattern)));
        Assert.Equal(1000, item.Id);
    }

    [ConditionalFact]
    public virtual void Where_Array_Any_StartsWith_Missing_Returns_No_Results()
    {
        using ArrayQueryContext context = CreateContext();
        bool any = context.TableItems.Any(i => i.StringArray.Any(a => a.StartsWith("zzz")));
        Assert.False(any);
    }

    [ConditionalFact]
    public virtual void Where_Array_Any_Contains_Escapes_Special_Characters()
    {
        using ArrayQueryContext context = CreateContext();
        bool any = context.TableItems.Any(i => i.StringArray.Any(a => a.Contains("5_")));
        Assert.False(any);
    }

    [ConditionalFact]
    public virtual void Where_Array_All_Contains_Empty_Pattern_Matches_All_Elements()
    {
        using ArrayQueryContext context = CreateContext();
        TableItem item = context.TableItems.Single(i => i.StringArray.All(a => a.Contains("")));
        Assert.Equal(1000, item.Id);
    }

    [ConditionalFact]
    public virtual void Where_Array_All_StartsWith_Not_All_Match_Returns_No_Results()
    {
        using ArrayQueryContext context = CreateContext();
        bool any = context.TableItems.Any(i => i.StringArray.All(a => a.StartsWith("a")));
        Assert.False(any);
    }

    [ConditionalFact]
    public virtual void Where_Array_All_On_Empty_Array_Is_Vacuously_True()
    {
        using ArrayQueryContext context = CreateContext();
        bool clientSideAll = Array.Empty<string>().All(a => a.StartsWith("a"));
        Assert.True(clientSideAll);

        TableColumnNullable item = context.TableColumnNullables.Single(i => i.Id == 3);
        Assert.NotNull(item.StringArray);
        Assert.Empty(item.StringArray);

        bool dbSideAll = context.TableColumnNullables
            .Any(i => i.Id == 3 && i.StringArray!.All(a => a.StartsWith("a")));

        Assert.Equal(clientSideAll, dbSideAll);
    }

    [ConditionalFact]
    public virtual void Where_Array_All_On_Null_Array_Is_Vacuously_True()
    {
        using ArrayQueryContext context = CreateContext();

        TableColumnNullable item = context.TableColumnNullables.Single(i => i.Id == 2);
        Assert.Null(item.StringArray);

        bool dbSideAll = context.TableColumnNullables
            .Any(i => i.Id == 2 && i.StringArray!.All(a => a.StartsWith("a")));

        Assert.True(dbSideAll);
    }

    [ConditionalFact]
    public virtual void Where_Array_Any_Parameterless_On_NonEmpty_Array_Is_True()
    {
        using ArrayQueryContext context = CreateContext();
        bool any = context.TableItems.Any(i => i.StringArray.Any());
        Assert.True(any);
    }

    [ConditionalFact]
    public virtual void Where_Array_Any_Parameterless_On_Empty_Array_Is_False()
    {
        using ArrayQueryContext context = CreateContext();
        bool any = context.TableColumnNullables.Any(i => i.Id == 3 && i.StringArray!.Any());
        Assert.False(any);
    }

    [ConditionalFact]
    public virtual void Where_Array_Any_Parameterless_On_Null_Array_Is_False()
    {
        using ArrayQueryContext context = CreateContext();
        bool any = context.TableColumnNullables.Any(i => i.Id == 2 && i.StringArray!.Any());
        Assert.False(any);
    }

    [ConditionalFact]
    public virtual void Where_Array_Any_Compound_Predicate_Is_Not_Supported()
    {
        using ArrayQueryContext context = CreateContext();
        Assert.Throws<InvalidOperationException>(
            () => context.TableItems.Any(i => i.StringArray.Any(a => a.StartsWith("a") && a.EndsWith("b"))));
    }

    [ConditionalFact]
    public virtual void Where_Array_Any_Non_String_Method_Predicate_Is_Not_Supported()
    {
        using ArrayQueryContext context = CreateContext();
        Assert.Throws<InvalidOperationException>(
            () => context.TableItems.Any(i => i.StringArray.Any(a => a.Length > 2)));
    }

    [ConditionalFact]
    public virtual void Where_Array_Any_Nested_Member_Chain_Predicate_Is_Not_Supported()
    {
        using ArrayQueryContext context = CreateContext();
        Assert.Throws<InvalidOperationException>(
            () => context.TableItems.Any(i => i.StringArray.Any(a => a.ToUpper().Contains("A"))));
    }

    [ConditionalFact]
    public virtual void Where_Array_Any_Chained_Before_Predicate_Is_Not_Supported()
    {
        using ArrayQueryContext context = CreateContext();
        Assert.Throws<InvalidOperationException>(
            () => context.TableItems.Any(i => i.StringArray.Where(a => a.Length > 1).Any()));
    }

    protected ArrayQueryContext CreateContext() => Fixture.CreateContext();

    public class TableItem
    {
        public long Id { get; set; }
        public string[] StringArray { get; set; } = null!;
        public List<string> StringList { get; set; } = null!;
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
                StringArray = ["a", "B", "CCCC", "5\\X"],
                StringList = ["a", "B", "CCCC"],
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
            new TableColumnNullable()
            {
                Id = 3,
                StringArray = []
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

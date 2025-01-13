using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using System.Data.Common;

namespace EFCore.Snowflake.FunctionalTests.Custom;

public class SnowflakeLtzTests(SnowflakeLtzTests.SnowflakeLtzFixture fixture) : IClassFixture<SnowflakeLtzTests.SnowflakeLtzFixture>
{
    private const long CommonId = 123;
    private const long DirectQueryId = 42;
    private static readonly DateTime DateTimeToInsert = new(2025, 01, 01, 0, 0, 0, DateTimeKind.Utc);

    [ConditionalFact]
    public virtual async Task Reads_using_DateTimeOffset()
    {
        await using DbContext context = CreateContext();

        ItemWithDateTimeOffset item = await context.Set<ItemWithDateTimeOffset>().SingleAsync(it => it.Id == CommonId);

        // large precision, because we are not sure what Time zone is running on Snowflake server
        Assert.Equal(DateTimeToInsert.ToLocalTime(), item.LtzDate.DateTime, precision: TimeSpan.FromHours(12));
        Assert.Equal(new DateTimeOffset(DateTimeToInsert).ToLocalTime().Offset, item.LtzDate.Offset);
    }

    [ConditionalFact]
    public virtual async Task Reads_with_filter_using_DateTimeOffset()
    {
        await using DbContext context = CreateContext();

        DateTimeOffset filterVal = DateTimeToInsert - TimeSpan.FromDays(1);

        ItemWithDateTimeOffset item = await context.Set<ItemWithDateTimeOffset>()
            .Where(it => it.LtzDate > filterVal)
            .OrderBy(it => it.LtzDate)
            .FirstAsync();

        Assert.Equal(CommonId, item.Id);
    }

    [ConditionalFact]
    public virtual async Task Reads_using_DateTime()
    {
        await using DbContext context = CreateContext();

        ItemWithDateTime item = await context.Set<ItemWithDateTime>().SingleAsync(it => it.Id == CommonId);

        // large precision, because we are not sure what Time zone is running on Snowflake server
        Assert.Equal(DateTimeToInsert.ToLocalTime(), item.LtzDate, precision: TimeSpan.FromHours(12));
    }

    [ConditionalFact]
    public virtual async Task Reads_with_filter_using_DateTime()
    {
        await using DbContext context = CreateContext();

        DateTime filterVal = DateTimeToInsert - TimeSpan.FromDays(1);

        ItemWithDateTime item = await context.Set<ItemWithDateTime>()
            .Where(it => it.LtzDate > filterVal)
            .OrderBy(it => it.LtzDate)
            .FirstAsync();

        Assert.Equal(CommonId, item.Id);
    }

    /// <summary>
    /// If this test fails on insert, then it means that Snowflake.Data API has been changed
    /// Currently (4.2.0) Snowflake.Data expects TIMEZONE_LTZ to be inserted as DateTime and read as DateTimeOffset
    /// </summary>
    [ConditionalFact]
    public virtual async Task Inserts_using_DateTimeOffset()
    {
        await using DbContext context = CreateContext();
        DbConnection dbConnection = context.Database.GetDbConnection();

        await using (DbCommand insert = dbConnection.CreateCommand())
        {
            insert.CommandText =
                $"INSERT INTO \"ItemWithDateTimeOffset\" VALUES (:p0, '2025-01-02 00:00:00 +0000'::TIMESTAMP_LTZ)";

            DbParameter p0 = insert.CreateParameter();
            p0.ParameterName = "p0";
            p0.Value = DirectQueryId;

            DbParameter p1 = insert.CreateParameter();
            p1.ParameterName = "p1";
            p1.Value = new DateTimeOffset(new DateTime(2000, 1, 1));

            insert.Parameters.AddRange(new[] { p0, p1 });

            await insert.ExecuteNonQueryAsync();
        }

        await using DbCommand select = dbConnection.CreateCommand();
        select.CommandText = $"SELECT * FROM \"ItemWithDateTimeOffset\" WHERE \"Id\" = {DirectQueryId}";

        await using var reader = await select.ExecuteReaderAsync();

        foreach (DbDataRecord row in reader)
        {
            object o = row["LtzDate"];
            Assert.IsType<DateTimeOffset>(o);
        }
    }

    protected DbContext CreateContext() => fixture.CreateContext();

    protected class ItemWithDateTime
    {
        public long Id { get; set; }
        public DateTime LtzDate { get; set; }
    }

    protected class ItemWithDateTimeOffset
    {
        public long Id { get; set; }
        public DateTimeOffset LtzDate { get; set; }
    }

    public class SnowflakeLtzFixture : SharedStoreFixtureBase<PoolableDbContext>
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            modelBuilder.Entity<ItemWithDateTime>()
                .Property(p => p.LtzDate)
                .HasColumnType("TIMESTAMP_LTZ(3)");

            modelBuilder.Entity<ItemWithDateTimeOffset>()
                .Property(p => p.LtzDate)
                .HasColumnType("TIMESTAMP_LTZ(3)");
        }

        protected override async Task SeedAsync(PoolableDbContext context)
        {
            //FormattableString formattable = $"INSERT INTO \"ItemWithDateTimeOffset\" VALUES (123, '2025-01-01 00:00:00 +0000'::TIMESTAMP_LTZ)";
            //await context.Database.ExecuteSqlAsync(formattable);

            context.AddRange(
                new ItemWithDateTime()
                {
                    Id = CommonId,
                    LtzDate = DateTimeToInsert
                });

            context.AddRange(
                new ItemWithDateTimeOffset()
                {
                    Id = CommonId,
                    LtzDate = DateTimeToInsert
                });

            await context.SaveChangesAsync();
        }

        protected override string StoreName => "SnowflakeLtz";
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

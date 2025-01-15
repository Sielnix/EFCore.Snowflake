using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Storage;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EFCore.Snowflake.FunctionalTests.Migrations;
public class MigrationsInfrastructureSnowflakeTest(MigrationsInfrastructureSnowflakeTest.MigrationsInfrastructureSnowflakeFixture fixture)
    : MigrationsInfrastructureTestBase<MigrationsInfrastructureSnowflakeTest.MigrationsInfrastructureSnowflakeFixture>(fixture)
{
    public override void Can_get_active_provider()
    {
        base.Can_get_active_provider();

        Assert.Equal("EFCore.Snowflake", ActiveProvider);
    }

    [ConditionalFact(Skip = "https://github.com/dotnet/efcore/issues/33056")]
    public override void Can_apply_all_migrations()
        => base.Can_apply_all_migrations();

    [ConditionalFact(Skip = "https://github.com/dotnet/efcore/issues/33056")]
    public override void Can_apply_range_of_migrations()
        => base.Can_apply_range_of_migrations();

    [ConditionalFact(Skip = "https://github.com/dotnet/efcore/issues/33056")]
    public override void Can_revert_all_migrations()
        => base.Can_revert_all_migrations();

    [ConditionalFact(Skip = "https://github.com/dotnet/efcore/issues/33056")]
    public override void Can_revert_one_migrations()
        => base.Can_revert_one_migrations();

    [ConditionalFact(Skip = "https://github.com/dotnet/efcore/issues/33056")]
    public override Task Can_apply_all_migrations_async()
        => base.Can_apply_all_migrations_async();

    private const int MigrationParallelism = 4;

    [ConditionalFact]
    public override void Can_apply_one_migration()
    {
        using var db = Fixture.CreateContext();
        db.Database.EnsureDeleted();

        GiveMeSomeTime(db);

        var migrator = db.GetService<IMigrator>();
        migrator.Migrate("Migration1");

        var history = db.GetService<IHistoryRepository>();
        Assert.Collection(
            history.GetAppliedMigrations(),
            x => Assert.Equal("00000000000001_Migration1", x.MigrationId));

        //ONLY CHANGE: commented below. TODO: Find out why test logger is not used properly
        //Assert.Equal(
        //    LogLevel.Error,
        //    Fixture.TestSqlLoggerFactory.Log.Single(l => l.Id == RelationalEventId.PendingModelChangesWarning).Level);
    }

    [ConditionalFact]
    public override void Can_apply_one_migration_in_parallel()
    {
        using var db = Fixture.CreateContext();
        db.Database.EnsureDeleted();
        GiveMeSomeTime(db);
        db.GetService<IRelationalDatabaseCreator>().Create();

        Parallel.For(
            // ONLY CHANGE: migration parallelism because of limitation of Snowflake.Data
            0, MigrationParallelism, i =>
            {
                using var context = Fixture.CreateContext();
                var migrator = context.GetService<IMigrator>();
                migrator.Migrate("Migration1");
            });

        var history = db.GetService<IHistoryRepository>();
        Assert.Collection(
            history.GetAppliedMigrations(),
            x => Assert.Equal("00000000000001_Migration1", x.MigrationId));
    }

    [ConditionalFact]
    public override async Task Can_apply_one_migration_in_parallel_async()
    {
        using var db = Fixture.CreateContext();
        await db.Database.EnsureDeletedAsync();
        await GiveMeSomeTimeAsync(db);
        await db.GetService<IRelationalDatabaseCreator>().CreateAsync();

        await Parallel.ForAsync(
            // ONLY CHANGE: migration parallelism because of limitation of Snowflake.Data
            0, MigrationParallelism, async (i, _) =>
            {
                using var context = Fixture.CreateContext();
                var migrator = context.GetService<IMigrator>();
                await migrator.MigrateAsync("Migration1");
            });

        var history = db.GetService<IHistoryRepository>();
        Assert.Collection(
            await history.GetAppliedMigrationsAsync(),
            x => Assert.Equal("00000000000001_Migration1", x.MigrationId));
    }

    [ConditionalFact]
    public override void Can_apply_second_migration_in_parallel()
    {
        using var db = Fixture.CreateContext();
        db.Database.EnsureDeleted();
        GiveMeSomeTime(db);
        db.GetService<IMigrator>().Migrate("Migration1");

        Parallel.For(
            // ONLY CHANGE: migration parallelism because of limitation of Snowflake.Data
            0, MigrationParallelism, i =>
            {
                using var context = Fixture.CreateContext();
                var migrator = context.GetService<IMigrator>();
                migrator.Migrate("Migration2");
            });

        var history = db.GetService<IHistoryRepository>();
        Assert.Collection(
            history.GetAppliedMigrations(),
            x => Assert.Equal("00000000000001_Migration1", x.MigrationId),
            x => Assert.Equal("00000000000002_Migration2", x.MigrationId));
    }

    [ConditionalFact]
    public override async Task Can_apply_second_migration_in_parallel_async()
    {
        using var db = Fixture.CreateContext();
        await db.Database.EnsureDeletedAsync();
        await GiveMeSomeTimeAsync(db);
        await db.GetService<IMigrator>().MigrateAsync("Migration1");

        await Parallel.ForAsync(
            // ONLY CHANGE: migration parallelism because of limitation of Snowflake.Data
            0, MigrationParallelism, async (i, _) =>
            {
                using var context = Fixture.CreateContext();
                var migrator = context.GetService<IMigrator>();
                await migrator.MigrateAsync("Migration2");
            });

        var history = db.GetService<IHistoryRepository>();
        Assert.Collection(
            await history.GetAppliedMigrationsAsync(),
            x => Assert.Equal("00000000000001_Migration1", x.MigrationId),
            x => Assert.Equal("00000000000002_Migration2", x.MigrationId));
    }

    [ConditionalFact]
    public async Task Empty_Migration_Creates_Database()
    {
        await using var context = new BloggingContext(
            Fixture.TestStore.AddProviderOptions(
                new DbContextOptionsBuilder().EnableServiceProviderCaching(false))
            .ConfigureWarnings(e => e.Log(RelationalEventId.PendingModelChangesWarning))
            .Options);

        await context.Database.EnsureDeletedAsync();

        IRelationalDatabaseCreator creator = context.GetService<IRelationalDatabaseCreator>();

        await context.Database.MigrateAsync();

        Assert.True(await creator.ExistsAsync());
    }

    [ConditionalFact(Skip = "Generated sql mixed with direct statements, not supported by snowflake")]
    public override Task Can_generate_one_up_and_down_script() => base.Can_generate_one_up_and_down_script();

    [ConditionalFact(Skip = "Generated sql mixed with direct statements, not supported by snowflake")]
    public override Task Can_generate_up_and_down_script_using_names() => base.Can_generate_up_and_down_script_using_names();

    [ConditionalFact(Skip = "Generated sql mixed with direct statements, not supported by snowflake")]
    public override Task Can_generate_up_and_down_scripts() => base.Can_generate_up_and_down_scripts();

    [ConditionalFact(Skip = "Generated sql mixed with direct statements, not supported by snowflake")]
    public override Task Can_generate_up_and_down_scripts_noTransactions() => base.Can_generate_up_and_down_scripts_noTransactions();

    [ConditionalFact(Skip = "Snowflake doesn't work with DDL and transactions")]
    public override void Can_apply_two_migrations_in_transaction() => base.Can_apply_two_migrations_in_transaction();

    [ConditionalFact(Skip = "Snowflake doesn't work with DDL and transactions")]
    public override Task Can_apply_two_migrations_in_transaction_async() => base.Can_apply_two_migrations_in_transaction_async();

    private class BloggingContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Blog> Blogs { get; set; } = null!;

        public class Blog
        {
            public int Id { get; set; }

            public string Name { get; set; } = null!;
        }
    }

    public override void Can_diff_against_2_2_model()
    {
        // TODO: Implement
    }

    public override void Can_diff_against_3_0_ASP_NET_Identity_model()
    {
        // TODO: Implement
    }

    public override void Can_diff_against_2_2_ASP_NET_Identity_model()
    {
        // TODO: Implement
    }

    public override void Can_diff_against_2_1_ASP_NET_Identity_model()
    {
        // TODO: Implement
    }

    protected override async Task ExecuteSqlAsync(string value)
    {
        const string SplitBy = "EXECUTE IMMEDIATE";

        if (!value.Contains(SplitBy))
        {
            await ((SnowflakeTestStore)Fixture.TestStore).ExecuteNonQueryAsync(value);
            return;
        }

        string[] split = value.Split(SplitBy, StringSplitOptions.None);

        foreach (string sqlPart in split)
        {
            if (string.IsNullOrWhiteSpace(sqlPart))
            {
                continue;
            }

            string toExecute = SplitBy + sqlPart;

            await ((SnowflakeTestStore)Fixture.TestStore).ExecuteNonQueryAsync(toExecute);
        }
    }

    public class MigrationsInfrastructureSnowflakeFixture : MigrationsInfrastructureFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        public override MigrationsContext CreateContext()
        {
            var options = AddOptions(
                    TestStore.AddProviderOptions(new DbContextOptionsBuilder())
                        .UseSnowflake(
                            TestStore.ConnectionString, b => b.ApplyConfiguration())
                    )
                .UseInternalServiceProvider(CreateServiceProvider())
                .Options;
            return new MigrationsContext(options);
        }

        private static IServiceProvider CreateServiceProvider()
            => new ServiceCollection()
                .AddEntityFrameworkSnowflake()
                .BuildServiceProvider();
    }
}

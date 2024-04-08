using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Storage;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

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

    [ConditionalFact]
    public async Task Empty_Migration_Creates_Database()
    {
        DbContextOptionsBuilder builder = Fixture.TestStore.AddProviderOptions(
            new DbContextOptionsBuilder().EnableServiceProviderCaching(false));

        DbContextOptions options = builder.Options;

        await using BloggingContext context = new BloggingContext(options);

        var creator = (SnowflakeDatabaseCreator)context.GetService<IRelationalDatabaseCreator>();

        await context.Database.MigrateAsync();

        Assert.True(await creator.ExistsAsync());
    }

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

    public class MigrationsInfrastructureSnowflakeFixture : MigrationsInfrastructureFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        public override MigrationsContext CreateContext()
        {
            var options = AddOptions(
                    new DbContextOptionsBuilder()
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

using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Storage;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Snowflake.FunctionalTests.Migrations;

public class CustomSchemaMigrationsTest : IClassFixture<CustomSchemaMigrationsTest.CustomSchemaMigrationsFixture>
{
    public CustomSchemaMigrationsTest(CustomSchemaMigrationsFixture fixture)
    {
        Fixture = fixture;
        Fixture.TestStore.CloseConnection();
    }

    protected CustomSchemaMigrationsFixture Fixture { get; }


    [ConditionalFact]
    public async Task Migration_runs_and_check_on_custom_schema()
    {
        async Task RunMigration()
        {
            await using var context = new BloggingContext(
                Fixture.TestStore.AddProviderOptions(
                    new DbContextOptionsBuilder().EnableServiceProviderCaching(false)).Options);

            SnowflakeDatabaseCreator creator = (SnowflakeDatabaseCreator)context.GetService<IRelationalDatabaseCreator>();

            await context.Database.MigrateAsync();

            Assert.True(await creator.ExistsAsync());
        }

        await RunMigration();
        await RunMigration();
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

    public class CustomSchemaMigrationsFixture : MigrationsInfrastructureFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => new SnowflakeTestStoreFactory(schemaOverride: "DIFFICULT");

        protected override string StoreName => "CustomSchemaMigrations";

        public override MigrationsContext CreateContext()
        {
            var options = AddOptions(
                    new DbContextOptionsBuilder()
                        .UseSnowflake(
                            TestStore.ConnectionString, b => b.ApplyConfiguration()))
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

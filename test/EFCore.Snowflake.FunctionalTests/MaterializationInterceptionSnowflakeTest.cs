using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Snowflake.FunctionalTests;

public class MaterializationInterceptionSnowflakeTest :
    MaterializationInterceptionTestBase<MaterializationInterceptionSnowflakeTest.SnowflakeLibraryContext>,
    IClassFixture<MaterializationInterceptionSnowflakeTest.MaterializationInterceptionSnowflakeFixture>
{
    public MaterializationInterceptionSnowflakeTest(MaterializationInterceptionSnowflakeFixture fixture)
        : base(fixture)
    {
    }

    public class SnowflakeLibraryContext : LibraryContext
    {
        public SnowflakeLibraryContext(DbContextOptions options)
            : base(options)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<TestEntity30244>().OwnsMany(e => e.Settings);
        }
    }

    public override LibraryContext CreateContext(IEnumerable<ISingletonInterceptor> interceptors, bool inject)
        => new SnowflakeLibraryContext(Fixture.CreateOptions(interceptors, inject));

    public class MaterializationInterceptionSnowflakeFixture : SingletonInterceptorsFixtureBase
    {
        protected override string StoreName
            => "MaterializationInterception";

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        protected override IServiceCollection InjectInterceptors(
            IServiceCollection serviceCollection,
            IEnumerable<ISingletonInterceptor> injectedInterceptors)
            => base.InjectInterceptors(serviceCollection.AddEntityFrameworkSnowflake(), injectedInterceptors);

        public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
        {
            new SnowflakeDbContextOptionsBuilder(base.AddOptions(builder));
            return builder;
        }
    }
}

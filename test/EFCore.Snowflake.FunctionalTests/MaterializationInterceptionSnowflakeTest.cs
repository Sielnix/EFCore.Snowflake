using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class MaterializationInterceptionSnowflakeTest :
    MaterializationInterceptionTestBase<MaterializationInterceptionSnowflakeTest.SnowflakeLibraryContext>//,
    //IClassFixture<MaterializationInterceptionSnowflakeTest.MaterializationInterceptionSnowflakeFixture>
{
    //public MaterializationInterceptionSnowflakeTest(MaterializationInterceptionSnowflakeFixture fixture)
    //    : base(fixture)
    //{
    //}

    public class SnowflakeLibraryContext(DbContextOptions options) : LibraryContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<TestEntity30244>().OwnsMany(e => e.Settings);
        }
    }

    protected override ITestStoreFactory TestStoreFactory
        => SnowflakeTestStoreFactory.Instance;

    //public override LibraryContext CreateContext(IEnumerable<ISingletonInterceptor> interceptors, bool inject)
    //    => new SnowflakeLibraryContext(Fixture.CreateOptions(interceptors, inject));

    //public class MaterializationInterceptionSnowflakeFixture : SingletonInterceptorsFixtureBase
    //{
    //    protected override string StoreName
    //        => "MaterializationInterception";

    //    protected override ITestStoreFactory TestStoreFactory
    //        => SnowflakeTestStoreFactory.Instance;

    //    protected override IServiceCollection InjectInterceptors(
    //        IServiceCollection serviceCollection,
    //        IEnumerable<ISingletonInterceptor> injectedInterceptors)
    //        => base.InjectInterceptors(serviceCollection.AddEntityFrameworkSnowflake(), injectedInterceptors);

    //    public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
    //    {
    //        new SnowflakeDbContextOptionsBuilder(base.AddOptions(builder));
    //        return builder;
    //    }
    //}
}

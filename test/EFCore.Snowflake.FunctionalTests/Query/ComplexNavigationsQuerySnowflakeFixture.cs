using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class ComplexNavigationsQuerySnowflakeFixture : ComplexNavigationsQueryRelationalFixtureBase
{
    protected override ITestStoreFactory TestStoreFactory => SnowflakeTestStoreFactory.Instance;
    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        modelBuilder.HasDefaultSchema("PUBLIC");
        base.OnModelCreating(modelBuilder, context);
    }
}

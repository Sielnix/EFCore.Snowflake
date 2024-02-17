using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.ComplexNavigationsModel;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class ComplexNavigationsSharedTypeQuerySnowflakeFixture : ComplexNavigationsSharedTypeQueryRelationalFixtureBase
{
    protected override ITestStoreFactory TestStoreFactory => SnowflakeTestStoreFactory.Instance;

    protected override void Configure(OwnedNavigationBuilder<Level1, Level2> l2)
    {
        base.Configure(l2);
    }
}

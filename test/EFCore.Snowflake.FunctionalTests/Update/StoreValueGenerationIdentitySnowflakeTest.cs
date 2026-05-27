using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestModels.StoreValueGenerationModel;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.Update;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Update;

public class StoreValueGenerationIdentitySnowflakeTest
    : StoreValueGenerationTestBase<
        StoreValueGenerationIdentitySnowflakeTest.StoreValueGenerationIdentitySnowflakeFixture>
{

    public StoreValueGenerationIdentitySnowflakeTest(
        StoreValueGenerationIdentitySnowflakeFixture fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    protected override int ShouldExecuteInNumberOfCommands(EntityState firstOperationType, EntityState? secondOperationType,
        GeneratedValues generatedValues, bool withSameEntityType)
    {
        int commands = 0;
        if ((firstOperationType == EntityState.Added || firstOperationType == EntityState.Modified) && generatedValues != GeneratedValues.None)
        {
            commands += 2;
        }
        else if (firstOperationType != EntityState.Unchanged)
        {
            commands++;
        }

        if ((secondOperationType == EntityState.Added || secondOperationType == EntityState.Modified) && generatedValues != GeneratedValues.None)
        {
            commands += 2;
        }
        else if (secondOperationType.HasValue && secondOperationType != EntityState.Unchanged)
        {
            commands++;
        }

        return commands;
    }

    protected override bool ShouldCreateImplicitTransaction(
        EntityState firstOperationType,
        EntityState? secondOperationType,
        GeneratedValues generatedValues,
        bool withSameEntityType)
    {
        return ShouldExecuteInNumberOfCommands(
            firstOperationType,
            secondOperationType,
            generatedValues,
            withSameEntityType) > 1;
    }

    public class StoreValueGenerationIdentitySnowflakeFixture : StoreValueGenerationSnowflakeFixtureBase
    {
        protected override string StoreName
            => "StoreValueGenerationIdentityTest";

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            ISqlGenerationHelper sqlGenerationHelper = context.GetService<ISqlGenerationHelper>();

            foreach (var name in new[]
                     {
                         nameof(StoreValueGenerationContext.WithSomeDatabaseGenerated),
                         nameof(StoreValueGenerationContext.WithSomeDatabaseGenerated2)
                     })
            {
                modelBuilder
                    .SharedTypeEntity<StoreValueGenerationData>(name)
                    .Property(w => w.Data1)
                    .HasColumnType("NUMBER(11,0)") // CHANGE - Snowflake increases computed columns precision by 1, so we need to increase it here as well to pass the test
                    .HasComputedColumnSql(sqlGenerationHelper.DelimitIdentifier(nameof(StoreValueGenerationData.Id)) + " + 1");
            }
        }
    }
}

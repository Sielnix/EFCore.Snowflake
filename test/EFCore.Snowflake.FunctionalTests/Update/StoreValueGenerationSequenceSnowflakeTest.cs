using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestModels.StoreValueGenerationModel;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.Update;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Update;

public class StoreValueGenerationSequenceSnowflakeTest : StoreValueGenerationTestBase<
    StoreValueGenerationSequenceSnowflakeTest.StoreValueGenerationSequenceSnowflakeFixture>
{
    public StoreValueGenerationSequenceSnowflakeTest(
        StoreValueGenerationSequenceSnowflakeFixture fixture,
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
    
    public class StoreValueGenerationSequenceSnowflakeFixture : StoreValueGenerationSnowflakeFixtureBase
    {
        protected override string StoreName
            => "StoreValueGenerationSequenceTest";

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            modelBuilder.HasSequence<int>("Ids");

            foreach (var name in new[]
                     {
                         nameof(StoreValueGenerationContext.WithSomeDatabaseGenerated),
                         nameof(StoreValueGenerationContext.WithSomeDatabaseGenerated2),
                         nameof(StoreValueGenerationContext.WithAllDatabaseGenerated),
                         nameof(StoreValueGenerationContext.WithAllDatabaseGenerated2)
                     })
            {
                modelBuilder
                    .SharedTypeEntity<StoreValueGenerationData>(name)
                    .Property(w => w.Id)
                    .UseSequence("Ids");
            }
        }
    }
}

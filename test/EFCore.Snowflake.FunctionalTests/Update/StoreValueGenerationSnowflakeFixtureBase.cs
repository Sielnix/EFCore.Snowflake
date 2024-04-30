using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestModels.StoreValueGenerationModel;
using Microsoft.EntityFrameworkCore.Update;

namespace EFCore.Snowflake.FunctionalTests.Update;

public abstract class StoreValueGenerationSnowflakeFixtureBase : StoreValueGenerationFixtureBase
{
    public override void CleanData()
    {
        using var context = CreateContext();

        foreach (var query in GetCleanDataSql())
        {
            context.Database.ExecuteSqlRaw(query);
        }
    }

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
                .HasComputedColumnSql(sqlGenerationHelper.DelimitIdentifier(nameof(StoreValueGenerationData.Id)) + " + 1");
        }
    }

    private IEnumerable<string> GetCleanDataSql()
    {
        var context = CreateContext();

        var helper = context.GetService<ISqlGenerationHelper>();
        var tables = context.Model.GetEntityTypes()
            .SelectMany(e => e.GetTableMappings().Select(m => helper.DelimitIdentifier(m.Table.Name, m.Table.Schema)));

        foreach (var table in tables)
        {
            yield return $"TRUNCATE TABLE {table};";
        }
    }
}

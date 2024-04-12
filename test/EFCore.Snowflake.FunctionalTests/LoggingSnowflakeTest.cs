using EFCore.Snowflake.Diagnostics.Internal;
using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.Infrastructure;
using EFCore.Snowflake.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Diagnostics.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;

namespace EFCore.Snowflake.FunctionalTests;

public class LoggingSnowflakeTest : LoggingRelationalTestBase<SnowflakeDbContextOptionsBuilder, SnowflakeOptionsExtension>
{
    [ConditionalFact]
    public virtual void StoredProcedureConcurrencyTokenNotMapped_throws_by_default()
    {
        using var context = new StoredProcedureConcurrencyTokenNotMappedContext(CreateOptionsBuilder(new ServiceCollection()));

        var definition = RelationalResources.LogStoredProcedureConcurrencyTokenNotMapped(CreateTestLogger());
        Assert.Equal(
            CoreStrings.WarningAsErrorTemplate(
                RelationalEventId.StoredProcedureConcurrencyTokenNotMapped.ToString(),
                definition.GenerateMessage(nameof(Animal), "Animal_Update", nameof(Animal.Name)),
                "RelationalEventId.StoredProcedureConcurrencyTokenNotMapped"),
            Assert.Throws<InvalidOperationException>(
                () => context.Model).Message);
    }

    protected class StoredProcedureConcurrencyTokenNotMappedContext : DbContext
    {
        public StoredProcedureConcurrencyTokenNotMappedContext(DbContextOptionsBuilder optionsBuilder)
            : base(optionsBuilder.Options)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Animal>(
                b =>
                {
                    b.Ignore(a => a.FavoritePerson);
                    b.Property(e => e.Name).IsRowVersion();
                    b.UpdateUsingStoredProcedure(
                        b =>
                        {
                            b.HasOriginalValueParameter(e => e.Id);
                            b.HasParameter(e => e.Name, p => p.IsOutput());
                            b.HasRowsAffectedReturnValue();
                        });
                });
    }

    protected override DbContextOptionsBuilder CreateOptionsBuilder(
        IServiceCollection services,
        Action<RelationalDbContextOptionsBuilder<SnowflakeDbContextOptionsBuilder, SnowflakeOptionsExtension>> relationalAction)
        => new DbContextOptionsBuilder()
            .UseInternalServiceProvider(services.AddEntityFrameworkSnowflake().BuildServiceProvider(validateScopes: true))
            .UseSnowflake("Data Source=LoggingSnowflakeTest.db", relationalAction);

    protected override TestLogger CreateTestLogger()
        => new TestLogger<SnowflakeLoggingDefinitions>();

    protected override string ProviderName
        => "EFCore.Snowflake";

    protected override string ProviderVersion
        => typeof(SnowflakeOptionsExtension).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion!;
}

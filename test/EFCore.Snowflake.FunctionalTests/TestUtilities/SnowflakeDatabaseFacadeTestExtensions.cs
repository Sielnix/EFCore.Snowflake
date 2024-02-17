using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.FunctionalTests.TestUtilities;
public static class SnowflakeDatabaseFacadeTestExtensions
{
    public static void EnsureClean(this DatabaseFacade databaseFacade)
        => new SnowflakeDatabaseCleaner(databaseFacade.GetService<ISqlGenerationHelper>())
            .Clean(databaseFacade);
}

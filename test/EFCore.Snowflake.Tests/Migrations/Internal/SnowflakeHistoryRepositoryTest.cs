using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Snowflake.Tests.Migrations.Internal;

public class SnowflakeHistoryRepositoryTest
{
    [Fact]
    public void GetCreateScript_works()
    {
        var sql = CreateHistoryRepository().GetCreateScript();

        Assert.Equal(
            """
            EXECUTE IMMEDIATE
            $$
            BEGIN

                CREATE TABLE "__EFMigrationsHistory" (
                "MigrationId" VARCHAR(150) NOT NULL,
                "ProductVersion" VARCHAR(32) NOT NULL,
                CONSTRAINT "PK___EFMigrationsHistory" PRIMARY KEY ("MigrationId")
            );

            END;
            $$;

            """,
            sql,
            ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void GetCreateIfNotExistsScript_works()
    {
        var sql = CreateHistoryRepository().GetCreateIfNotExistsScript();

        Assert.Equal(
            """
            EXECUTE IMMEDIATE
            $$
            BEGIN

                CREATE TABLE IF NOT EXISTS "__EFMigrationsHistory" (
                "MigrationId" VARCHAR(150) NOT NULL,
                "ProductVersion" VARCHAR(32) NOT NULL,
                CONSTRAINT "PK___EFMigrationsHistory" PRIMARY KEY ("MigrationId")
            );

            END;
            $$;

            """,
            sql,
            ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void GetDeleteScript_works()
    {
        var sql = CreateHistoryRepository().GetDeleteScript("Migration1");

        Assert.Equal(
            """
            DELETE FROM "__EFMigrationsHistory"
            WHERE "MigrationId" = 'Migration1';

            """,
            sql,
            ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void GetInsertScript_works()
    {
        var sql = CreateHistoryRepository().GetInsertScript(new HistoryRow("Migration1", "7.0.0"));

        Assert.Equal(
            """
            INSERT INTO "__EFMigrationsHistory" ("MigrationId", "ProductVersion")
            VALUES ('Migration1', '7.0.0');

            """,
            sql,
            ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void GetBeginIfNotExistsScript_works()
    {
        var sql = CreateHistoryRepository().GetBeginIfNotExistsScript("Migration1");

        Assert.Equal(
            """
            EXECUTE IMMEDIATE
            $$
            DECLARE
                row_exists BOOLEAN;
            BEGIN
                SELECT
                    TO_BOOLEAN(COUNT(1))
                INTO
                    row_exists
                FROM "__EFMigrationsHistory"
                    WHERE "MigrationId" = 'Migration1';

                IF (row_exists = false) THEN
            """,
            sql,
            ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void GetBeginIfExistsScript_works()
    {
        var sql = CreateHistoryRepository().GetBeginIfExistsScript("Migration1");

        Assert.Equal(
            """
            EXECUTE IMMEDIATE
            $$
            DECLARE
                row_exists BOOLEAN;
            BEGIN
                SELECT
                    TO_BOOLEAN(COUNT(1))
                INTO
                    row_exists
                FROM "__EFMigrationsHistory"
                    WHERE "MigrationId" = 'Migration1';

                IF (row_exists) THEN
            """,
            sql,
            ignoreLineEndingDifferences: true);
    }

    [Fact]
    public void GetEndIfScript_works()
    {
        var sql = CreateHistoryRepository().GetEndIfScript();

        Assert.Equal(
            """
                END IF;
            END;
            $$;

            """,
            sql,
            ignoreLineEndingDifferences: true);
    }

    private static IHistoryRepository CreateHistoryRepository()
        => SnowflakeTestHelpers.Instance.CreateContextServices()
            .GetRequiredService<IHistoryRepository>();
}

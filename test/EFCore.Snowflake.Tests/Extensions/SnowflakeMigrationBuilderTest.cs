using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;

namespace EFCore.Snowflake.Tests.Extensions;
public class SnowflakeMigrationBuilderTest
{
    [Fact]
    public void IsSnowflake_when_using_Snowflake()
    {
        var migrationBuilder = new MigrationBuilder("EFCore.Snowflake");
        Assert.True(migrationBuilder.IsSnowflake());
    }

    [Fact]
    public void Not_IsSnowflake_when_using_different_provider()
    {
        var migrationBuilder = new MigrationBuilder("Microsoft.EntityFrameworkCore.InMemory");
        Assert.False(migrationBuilder.IsSnowflake());
    }
}

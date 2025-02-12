using EFCore.Snowflake.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

public static class SnowflakeOwnedNavigationTableBuilderExtensions
{
    public static void IsPermanent(this OwnedNavigationTableBuilder tableBuilder)
    {
        tableBuilder.Metadata.SetTableType(SnowflakeTableType.Permanent);
    }

    public static void IsTransient(this OwnedNavigationTableBuilder tableBuilder)
    {
        tableBuilder.Metadata.SetTableType(SnowflakeTableType.Transient);
    }

    public static void IsHybrid(this OwnedNavigationTableBuilder tableBuilder)
    {
        tableBuilder.Metadata.SetTableType(SnowflakeTableType.Hybrid);
    }
}

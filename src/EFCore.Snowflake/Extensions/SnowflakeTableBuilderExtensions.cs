using EFCore.Snowflake.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

// ReSharper disable once CheckNamespace

namespace Microsoft.EntityFrameworkCore;

public static class SnowflakeTableBuilderExtensions
{
    public static void IsPermanent(this TableBuilder tableBuilder)
    {
        tableBuilder.Metadata.SetTableType(SnowflakeTableType.Permanent);
    }

    public static void IsTransient(this TableBuilder tableBuilder)
    {
        tableBuilder.Metadata.SetTableType(SnowflakeTableType.Transient);
    }

    public static void IsHybrid(this TableBuilder tableBuilder)
    {
        tableBuilder.Metadata.SetTableType(SnowflakeTableType.Hybrid);
    }
}

using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

public static class SnowflakeEntityTypeExtensions
{
    public static SnowflakeTableType GetTableType(this IReadOnlyEntityType entityType)
        => entityType[SnowflakeAnnotationNames.TableType] as SnowflakeTableType? ?? SnowflakeTableType.Permanent;

    public static void SetTableType(this IMutableEntityType entityType, SnowflakeTableType tableType)
        => entityType.SetOrRemoveAnnotation(SnowflakeAnnotationNames.TableType, tableType);

    public static SnowflakeTableType? SetTableType(
        this IConventionEntityType entityType,
        SnowflakeTableType? temporal,
        bool fromDataAnnotation = false)
        => (SnowflakeTableType?)entityType.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.TableType,
            temporal,
            fromDataAnnotation)?.Value;

    public static ConfigurationSource? GetTableTypeConfigurationSource(this IConventionEntityType entityType)
        => entityType.FindAnnotation(SnowflakeAnnotationNames.TableType)?.GetConfigurationSource();
}

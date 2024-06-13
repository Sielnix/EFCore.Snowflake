using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

public static class SnowflakeEntityTypeBuilderExtensions
{
    public static IConventionEntityTypeBuilder? HasTableType(
        this IConventionEntityTypeBuilder entityTypeBuilder,
        SnowflakeTableType tableType,
        bool fromDataAnnotation = false)
    {
        if (entityTypeBuilder.CanSetTableType(tableType, fromDataAnnotation))
        {
            entityTypeBuilder.Metadata.SetTableType(tableType, fromDataAnnotation);

            return entityTypeBuilder;
        }

        return null;
    }

    public static bool CanSetTableType(
        this IConventionEntityTypeBuilder entityTypeBuilder,
        SnowflakeTableType tableType,
        bool fromDataAnnotation = false)
        => entityTypeBuilder.CanSetAnnotation(
            SnowflakeAnnotationNames.TableType, tableType, fromDataAnnotation);
}

using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Extensions;
internal static class SnowflakePropertyExtensions
{
    public static SnowflakeValueGenerationStrategy GetValueGenerationStrategy(
        this IReadOnlyProperty property,
        in StoreObjectIdentifier storeObject)
    {
        return GetValueGenerationStrategy(property, storeObject, null);
    }

    internal static SnowflakeValueGenerationStrategy GetValueGenerationStrategy(
        this IReadOnlyProperty property,
        in StoreObjectIdentifier storeObject,
        ITypeMappingSource? typeMappingSource)
    {
        IAnnotation? @override = property.FindOverrides(storeObject)?.FindAnnotation(SnowflakeAnnotationNames.ValueGenerationStrategy);
        if (@override != null)
        {
            return (SnowflakeValueGenerationStrategy?)@override.Value ?? SnowflakeValueGenerationStrategy.None;
        }

        IAnnotation? annotation = property.FindAnnotation(SnowflakeAnnotationNames.ValueGenerationStrategy);
        if (annotation?.Value != null
            && StoreObjectIdentifier.Create(property.DeclaringType, storeObject.StoreObjectType) == storeObject)
        {
            return (SnowflakeValueGenerationStrategy)annotation.Value;
        }

        var table = storeObject;
        var sharedTableRootProperty = property.FindSharedStoreObjectRootProperty(storeObject);
        if (sharedTableRootProperty != null)
        {
            return sharedTableRootProperty.GetValueGenerationStrategy(storeObject, typeMappingSource)
                == SnowflakeValueGenerationStrategy.AutoIncrement
                && table.StoreObjectType == StoreObjectType.Table
                && !property.GetContainingForeignKeys().Any(
                    fk =>
                        !fk.IsBaseLinking()
                        || (StoreObjectIdentifier.Create(fk.PrincipalEntityType, StoreObjectType.Table)
                                is StoreObjectIdentifier principal
                            && fk.GetConstraintName(table, principal) != null))
                    ? SnowflakeValueGenerationStrategy.AutoIncrement
                    : SnowflakeValueGenerationStrategy.None;
        }

        if (property.ValueGenerated != ValueGenerated.OnAdd
            || table.StoreObjectType != StoreObjectType.Table
            || property.TryGetDefaultValue(storeObject, out _)
            || property.GetDefaultValueSql(storeObject) != null
            || property.GetComputedColumnSql(storeObject) != null
            || property.GetContainingForeignKeys()
                .Any(
                    fk =>
                        !fk.IsBaseLinking()
                        || (StoreObjectIdentifier.Create(fk.PrincipalEntityType, StoreObjectType.Table)
                                is StoreObjectIdentifier principal
                            && fk.GetConstraintName(table, principal) != null)))
        {
            return SnowflakeValueGenerationStrategy.None;
        }

        var defaultStrategy = GetDefaultValueGenerationStrategy(property, storeObject, typeMappingSource);
        if (defaultStrategy != SnowflakeValueGenerationStrategy.None)
        {
            if (annotation != null)
            {
                return (SnowflakeValueGenerationStrategy?)annotation.Value ?? SnowflakeValueGenerationStrategy.None;
            }
        }

        return defaultStrategy;
    }

    private static SnowflakeValueGenerationStrategy GetDefaultValueGenerationStrategy(
        IReadOnlyProperty property,
        in StoreObjectIdentifier storeObject,
        ITypeMappingSource? typeMappingSource)
    {
        var modelStrategy = property.DeclaringType.Model.GetValueGenerationStrategy();

        return modelStrategy == SnowflakeValueGenerationStrategy.AutoIncrement
               && IsCompatibleAutoIncrementColumn(property, in storeObject, typeMappingSource)
            ? SnowflakeValueGenerationStrategy.AutoIncrement
            : SnowflakeValueGenerationStrategy.None;
    }

    private static bool IsCompatibleAutoIncrementColumn(
        IReadOnlyProperty property,
        in StoreObjectIdentifier storeObject,
        ITypeMappingSource? typeMappingSource)
    {
        if (storeObject.StoreObjectType != StoreObjectType.Table)
        {
            return false;
        }

        ValueConverter? valueConverter = GetConverter(property, storeObject, typeMappingSource);
        var type = (valueConverter?.ProviderClrType ?? property.ClrType).UnwrapNullableType();

        return (type.IsInteger()
                || type == typeof(decimal));
    }

    private static ValueConverter? GetConverter(
        IReadOnlyProperty property,
        StoreObjectIdentifier storeObject,
        ITypeMappingSource? typeMappingSource)
    {
        return property.GetValueConverter()
               ?? (property.FindRelationalTypeMapping(storeObject)
                   ?? typeMappingSource?.FindMapping((IProperty)property))?.Converter;
    }

    public static long? GetIdentitySeed(this IReadOnlyProperty property, in StoreObjectIdentifier storeObject)
    {
        if (property is RuntimeProperty)
        {
            throw new InvalidOperationException(CoreStrings.RuntimeModelMissingData);
        }

        var @override = property.FindOverrides(storeObject)?.FindAnnotation(SnowflakeAnnotationNames.IdentitySeed);
        if (@override != null)
        {
            return (long?)@override.Value;
        }

        var annotation = property.FindAnnotation(SnowflakeAnnotationNames.IdentitySeed);
        if (annotation is not null)
        {
            return annotation.Value is int intValue
                ? intValue
                : (long?)annotation.Value;
        }

        var sharedProperty = property.FindSharedStoreObjectRootProperty(storeObject);
        return sharedProperty == null
            ? property.DeclaringType.Model.GetIdentitySeed()
            : sharedProperty.GetIdentitySeed(storeObject);
    }

    public static int? GetIdentityIncrement(this IReadOnlyProperty property, in StoreObjectIdentifier storeObject)
    {
        if (property is RuntimeProperty)
        {
            throw new InvalidOperationException(CoreStrings.RuntimeModelMissingData);
        }

        var @override = property.FindOverrides(storeObject)?.FindAnnotation(SnowflakeAnnotationNames.IdentityIncrement);
        if (@override != null)
        {
            return (int?)@override.Value;
        }

        var annotation = property.FindAnnotation(SnowflakeAnnotationNames.IdentityIncrement);
        if (annotation != null)
        {
            return (int?)annotation.Value;
        }

        var sharedProperty = property.FindSharedStoreObjectRootProperty(storeObject);
        return sharedProperty == null
            ? property.DeclaringType.Model.GetIdentityIncrement()
            : sharedProperty.GetIdentityIncrement(storeObject);
    }
}

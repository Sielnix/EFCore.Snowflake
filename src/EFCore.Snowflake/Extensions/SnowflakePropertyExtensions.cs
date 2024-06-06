using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using EFCore.Snowflake.Properties;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

public static class SnowflakePropertyExtensions
{
    public static string? GetSequenceName(this IReadOnlyProperty property)
        => (string?)property[SnowflakeAnnotationNames.SequenceName];

    public static string? GetSequenceName(this IReadOnlyProperty property, in StoreObjectIdentifier storeObject)
    {
        var annotation = property.FindAnnotation(SnowflakeAnnotationNames.SequenceName);
        if (annotation != null)
        {
            return (string?)annotation.Value;
        }

        return property.FindSharedStoreObjectRootProperty(storeObject)?.GetSequenceName(storeObject);
    }

    public static void SetSequenceName(this IMutableProperty property, string? name)
        => property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.SequenceName,
            Check.NullButNotEmpty(name, nameof(name)));

    public static string? SetSequenceName(
        this IConventionProperty property,
        string? name,
        bool fromDataAnnotation = false)
        => (string?)property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.SequenceName,
            Check.NullButNotEmpty(name, nameof(name)),
            fromDataAnnotation)?.Value;

    public static ConfigurationSource? GetSequenceNameConfigurationSource(this IConventionProperty property)
        => property.FindAnnotation(SnowflakeAnnotationNames.SequenceName)?.GetConfigurationSource();

    public static string? GetSequenceSchema(this IReadOnlyProperty property, in StoreObjectIdentifier storeObject)
    {
        var annotation = property.FindAnnotation(SnowflakeAnnotationNames.SequenceSchema);
        if (annotation != null)
        {
            return (string?)annotation.Value;
        }

        return property.FindSharedStoreObjectRootProperty(storeObject)?.GetSequenceSchema(storeObject);
    }

    public static void SetSequenceSchema(this IMutableProperty property, string? schema)
        => property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.SequenceSchema,
            Check.NullButNotEmpty(schema, nameof(schema)));

    public static string? SetSequenceSchema(
        this IConventionProperty property,
        string? schema,
        bool fromDataAnnotation = false)
        => (string?)property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.SequenceSchema,
            Check.NullButNotEmpty(schema, nameof(schema)),
            fromDataAnnotation)?.Value;

    public static ConfigurationSource? GetSequenceSchemaConfigurationSource(this IConventionProperty property)
        => property.FindAnnotation(SnowflakeAnnotationNames.SequenceSchema)?.GetConfigurationSource();

    public static SnowflakeValueGenerationStrategy GetValueGenerationStrategy(this IReadOnlyProperty property)
    {
        var annotation = property.FindAnnotation(SnowflakeAnnotationNames.ValueGenerationStrategy);
        if (annotation != null)
        {
            return (SnowflakeValueGenerationStrategy?)annotation.Value ?? SnowflakeValueGenerationStrategy.None;
        }

        var defaultValueGenerationStrategy = GetDefaultValueGenerationStrategy(property);

        if (property.ValueGenerated != ValueGenerated.OnAdd
            || property.IsForeignKey()
            || property.TryGetDefaultValue(out _)
            || (defaultValueGenerationStrategy != SnowflakeValueGenerationStrategy.Sequence && property.GetDefaultValueSql() != null)
            || property.GetComputedColumnSql() != null)
        {
            return SnowflakeValueGenerationStrategy.None;
        }

        return defaultValueGenerationStrategy;
    }

    public static SnowflakeValueGenerationStrategy GetValueGenerationStrategy(
        this IReadOnlyProperty property,
        in StoreObjectIdentifier storeObject)
        => GetValueGenerationStrategy(property, storeObject, null);

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

    public static SnowflakeValueGenerationStrategy? GetValueGenerationStrategy(
        this IReadOnlyRelationalPropertyOverrides overrides)
        => (SnowflakeValueGenerationStrategy?)overrides.FindAnnotation(SnowflakeAnnotationNames.ValueGenerationStrategy)
            ?.Value;

    public static void SetValueGenerationStrategy(
        this IMutableProperty property,
        SnowflakeValueGenerationStrategy? value)
        => property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.ValueGenerationStrategy,
            CheckValueGenerationStrategy(property, value));

    public static SnowflakeValueGenerationStrategy? SetValueGenerationStrategy(
        this IConventionProperty property,
        SnowflakeValueGenerationStrategy? value,
        bool fromDataAnnotation = false)
        => (SnowflakeValueGenerationStrategy?)property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.ValueGenerationStrategy,
            CheckValueGenerationStrategy(property, value),
            fromDataAnnotation)?.Value;

    public static void SetValueGenerationStrategy(
        this IMutableRelationalPropertyOverrides overrides,
        SnowflakeValueGenerationStrategy? value)
        => overrides.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.ValueGenerationStrategy,
            CheckValueGenerationStrategy(overrides.Property, value));

    /// <summary>
    ///     Sets the <see cref="SnowflakeValueGenerationStrategy" /> to use for the property for a particular table.
    /// </summary>
    /// <param name="overrides">The property overrides.</param>
    /// <param name="value">The strategy to use.</param>
    /// <param name="fromDataAnnotation">Indicates whether the configuration was specified using a data annotation.</param>
    /// <returns>The configured value.</returns>
    public static SnowflakeValueGenerationStrategy? SetValueGenerationStrategy(
        this IConventionRelationalPropertyOverrides overrides,
        SnowflakeValueGenerationStrategy? value,
        bool fromDataAnnotation = false)
        => (SnowflakeValueGenerationStrategy?)overrides.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.ValueGenerationStrategy,
            CheckValueGenerationStrategy(overrides.Property, value),
            fromDataAnnotation)?.Value;

    /// <summary>
    ///     Returns the <see cref="ConfigurationSource" /> for the <see cref="SnowflakeValueGenerationStrategy" />.
    /// </summary>
    /// <param name="property">The property.</param>
    /// <returns>The <see cref="ConfigurationSource" /> for the <see cref="SnowflakeValueGenerationStrategy" />.</returns>
    public static ConfigurationSource? GetValueGenerationStrategyConfigurationSource(
        this IConventionProperty property)
        => property.FindAnnotation(SnowflakeAnnotationNames.ValueGenerationStrategy)?.GetConfigurationSource();

    /// <summary>
    ///     Returns the <see cref="ConfigurationSource" /> for the <see cref="SnowflakeValueGenerationStrategy" /> for a particular table.
    /// </summary>
    /// <param name="overrides">The property overrides.</param>
    /// <returns>The <see cref="ConfigurationSource" /> for the <see cref="SnowflakeValueGenerationStrategy" />.</returns>
    public static ConfigurationSource? GetValueGenerationStrategyConfigurationSource(
        this IConventionRelationalPropertyOverrides overrides)
        => overrides.FindAnnotation(SnowflakeAnnotationNames.ValueGenerationStrategy)?.GetConfigurationSource();

    private static SnowflakeValueGenerationStrategy GetDefaultValueGenerationStrategy(IReadOnlyProperty property)
    {
        SnowflakeValueGenerationStrategy? modelStrategy = property.DeclaringType.Model.GetValueGenerationStrategy();

        if (
            !modelStrategy.HasValue
            || modelStrategy.Value == SnowflakeValueGenerationStrategy.None
            || !IsCompatibleWithValueGeneration(property))
        {
            return SnowflakeValueGenerationStrategy.None;
        }

        return modelStrategy.Value;
    }

    private static SnowflakeValueGenerationStrategy GetDefaultValueGenerationStrategy(
        IReadOnlyProperty property,
        in StoreObjectIdentifier storeObject,
        ITypeMappingSource? typeMappingSource)
    {
        var modelStrategy = property.DeclaringType.Model.GetValueGenerationStrategy();

        if (modelStrategy is SnowflakeValueGenerationStrategy.Sequence && IsCompatibleWithValueGeneration(property))
        {
            return modelStrategy.Value;
        }

        return modelStrategy == SnowflakeValueGenerationStrategy.AutoIncrement
               && IsCompatibleWithValueGeneration(property, in storeObject, typeMappingSource)
            ? SnowflakeValueGenerationStrategy.AutoIncrement
            : SnowflakeValueGenerationStrategy.None;
    }

    private static SnowflakeValueGenerationStrategy? CheckValueGenerationStrategy(IReadOnlyProperty property, SnowflakeValueGenerationStrategy? value)
    {
        if (value is null)
        {
            return null;
        }

        var propertyType = property.ClrType;

        if ((value == SnowflakeValueGenerationStrategy.AutoIncrement)
            && !IsCompatibleWithValueGeneration(property))
        {
            throw new ArgumentException(
                SnowflakeStrings.IdentityBadType(
                    property.Name, property.DeclaringType.DisplayName(), propertyType.ShortDisplayName()));
        }

        if (value == SnowflakeValueGenerationStrategy.Sequence
            && !IsCompatibleWithValueGeneration(property))
        {
            throw new ArgumentException(
                SnowflakeStrings.SequenceBadType(
                    property.Name, property.DeclaringType.DisplayName(), propertyType.ShortDisplayName()));
        }

        return value;
    }

    public static bool IsCompatibleWithValueGeneration(IReadOnlyProperty property)
    {
        var valueConverter = property.GetValueConverter()
                             ?? property.FindTypeMapping()?.Converter;

        var type = (valueConverter?.ProviderClrType ?? property.ClrType).UnwrapNullableType();
        return type.IsInteger()
               || type.IsEnum
               || type == typeof(decimal);
    }

    private static bool IsCompatibleWithValueGeneration(
        IReadOnlyProperty property,
        in StoreObjectIdentifier storeObject,
        ITypeMappingSource? typeMappingSource)
    {
        if (storeObject.StoreObjectType != StoreObjectType.Table)
        {
            return false;
        }

        var valueConverter = property.GetValueConverter()
                             ?? (property.FindRelationalTypeMapping(storeObject)
                                 ?? typeMappingSource?.FindMapping((IProperty)property))?.Converter;

        var type = (valueConverter?.ProviderClrType ?? property.ClrType).UnwrapNullableType();

        return (type.IsInteger()
                || type.IsEnum
                || type == typeof(decimal));
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

    public static void SetIdentitySeed(this IMutableProperty property, long? seed)
        => property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentitySeed,
            seed);

    public static long? SetIdentitySeed(
        this IConventionProperty property,
        long? seed,
        bool fromDataAnnotation = false)
        => (long?)property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentitySeed,
            seed,
            fromDataAnnotation)?.Value;

    public static void SetIdentitySeed(this IMutableRelationalPropertyOverrides overrides, long? seed)
        => overrides.SetOrRemoveAnnotation(SnowflakeAnnotationNames.IdentitySeed, seed);

    public static ConfigurationSource? GetIdentitySeedConfigurationSource(this IConventionProperty property)
        => property.FindAnnotation(SnowflakeAnnotationNames.IdentitySeed)?.GetConfigurationSource();

    public static ConfigurationSource? GetIdentitySeedConfigurationSource(
        this IConventionProperty property,
        in StoreObjectIdentifier storeObject)
        => property.FindOverrides(storeObject)?.GetIdentitySeedConfigurationSource();

    public static ConfigurationSource? GetIdentitySeedConfigurationSource(
        this IConventionRelationalPropertyOverrides overrides)
        => overrides.FindAnnotation(SnowflakeAnnotationNames.IdentitySeed)?.GetConfigurationSource();

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

    public static void SetIdentityIncrement(this IMutableProperty property, int? increment)
        => property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentityIncrement,
            increment);

    public static bool? GetIdentityIsOrdered(this IReadOnlyProperty property, in StoreObjectIdentifier storeObject)
    {
        if (property is RuntimeProperty)
        {
            throw new InvalidOperationException(CoreStrings.RuntimeModelMissingData);
        }

        var @override = property.FindOverrides(storeObject)?.FindAnnotation(SnowflakeAnnotationNames.IdentityIsOrdered);
        if (@override != null)
        {
            return (bool?)@override.Value;
        }

        var annotation = property.FindAnnotation(SnowflakeAnnotationNames.IdentityIsOrdered);
        if (annotation != null)
        {
            return (bool?)annotation.Value;
        }

        var sharedProperty = property.FindSharedStoreObjectRootProperty(storeObject);
        return sharedProperty == null
            ? property.DeclaringType.Model.GetIdentityIsOrdered()
            : sharedProperty.GetIdentityIsOrdered(storeObject);
    }

    public static void SetIdentityIsOrdered(this IMutableProperty property, bool? ordered)
        => property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentityIsOrdered,
            ordered);

    public static bool? SetIdentityIsOrdered(
        this IConventionProperty property,
        bool? ordered,
        bool fromDataAnnotation = false)
        => (bool?)property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentityIsOrdered,
            ordered,
            fromDataAnnotation)?.Value;

    public static ConfigurationSource? GetIdentityIsOrderedConfigurationSource(this IConventionProperty property)
        => property.FindAnnotation(SnowflakeAnnotationNames.IdentityIsOrdered)?.GetConfigurationSource();

    public static int? SetIdentityIncrement(
        this IConventionProperty property,
        int? increment,
        bool fromDataAnnotation = false)
        => (int?)property.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentityIncrement,
            increment,
            fromDataAnnotation)?.Value;

    public static void SetIdentityIncrement(this IMutableRelationalPropertyOverrides overrides, int? increment)
        => overrides.SetOrRemoveAnnotation(SnowflakeAnnotationNames.IdentityIncrement, increment);

    public static void SetIdentityIsOrdered(this IMutableRelationalPropertyOverrides overrides, bool? ordered)
        => overrides.SetOrRemoveAnnotation(SnowflakeAnnotationNames.IdentityIsOrdered, ordered);

    public static ConfigurationSource? GetIdentityIsOrderedConfigurationSource(
        this IConventionProperty property,
        in StoreObjectIdentifier storeObject)
        => property.FindOverrides(storeObject)?.GetIdentityIsOrderedConfigurationSource();

    public static ConfigurationSource? GetIdentityIsOrderedConfigurationSource(
        this IConventionRelationalPropertyOverrides overrides)
        => overrides.FindAnnotation(SnowflakeAnnotationNames.IdentityIsOrdered)?.GetConfigurationSource();


    public static ConfigurationSource? GetIdentityIncrementConfigurationSource(this IConventionProperty property)
        => property.FindAnnotation(SnowflakeAnnotationNames.IdentityIncrement)?.GetConfigurationSource();

    public static ConfigurationSource? GetIdentityIncrementConfigurationSource(
        this IConventionProperty property,
        in StoreObjectIdentifier storeObject)
        => property.FindOverrides(storeObject)?.GetIdentityIncrementConfigurationSource();

    public static ConfigurationSource? GetIdentityIncrementConfigurationSource(
        this IConventionRelationalPropertyOverrides overrides)
        => overrides.FindAnnotation(SnowflakeAnnotationNames.IdentityIncrement)?.GetConfigurationSource();
}

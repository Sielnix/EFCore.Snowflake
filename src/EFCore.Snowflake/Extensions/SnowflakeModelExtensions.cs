using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

public static class SnowflakeModelExtensions
{
    public const string DefaultSequenceNameSuffix = "Sequence";

    public static SnowflakeValueGenerationStrategy? GetValueGenerationStrategy(this IReadOnlyModel model)
    {
        return (SnowflakeValueGenerationStrategy?)model[SnowflakeAnnotationNames.ValueGenerationStrategy];
    }

    public static void SetValueGenerationStrategy(
        this IMutableModel model,
        SnowflakeValueGenerationStrategy? value)
    {
        model.SetOrRemoveAnnotation(SnowflakeAnnotationNames.ValueGenerationStrategy, value);
    }

    public static SnowflakeIndexBehavior? GetIndexBehavior(this IReadOnlyModel model)
    {
        return (SnowflakeIndexBehavior?)model[SnowflakeAnnotationNames.IndexBehavior];
    }

    public static void SetIndexBehavior(
        this IMutableModel model,
        SnowflakeIndexBehavior? value)
    {
        model.SetOrRemoveAnnotation(SnowflakeAnnotationNames.IndexBehavior, value);
    }

    public static SnowflakeValueGenerationStrategy? SetValueGenerationStrategy(
        this IConventionModel model,
        SnowflakeValueGenerationStrategy? value,
        bool fromDataAnnotation = false)
    {
        return (SnowflakeValueGenerationStrategy?)model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.ValueGenerationStrategy,
            value,
            fromDataAnnotation)?.Value;
    }

    public static ConfigurationSource? GetValueGenerationStrategyConfigurationSource(this IConventionModel model)
        => model.FindAnnotation(SnowflakeAnnotationNames.ValueGenerationStrategy)?.GetConfigurationSource();

    public static string GetSequenceNameSuffix(this IReadOnlyModel model)
        => (string?)model[SnowflakeAnnotationNames.SequenceNameSuffix]
           ?? DefaultSequenceNameSuffix;

    public static void SetSequenceNameSuffix(this IMutableModel model, string? name)
    {
        Check.NullButNotEmpty(name, nameof(name));

        model.SetOrRemoveAnnotation(SnowflakeAnnotationNames.SequenceNameSuffix, name);
    }

    public static string? SetSequenceNameSuffix(
        this IConventionModel model,
        string? name,
        bool fromDataAnnotation = false)
        => (string?)model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.SequenceNameSuffix,
            Check.NullButNotEmpty(name, nameof(name)),
            fromDataAnnotation)?.Value;

    public static ConfigurationSource? GetSequenceNameSuffixConfigurationSource(this IConventionModel model)
        => model.FindAnnotation(SnowflakeAnnotationNames.SequenceNameSuffix)?.GetConfigurationSource();

    public static long? SetIdentitySeed(this IConventionModel model, long? seed, bool fromDataAnnotation = false)
        => (long?)model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentitySeed,
            seed,
            fromDataAnnotation)?.Value;

    public static void SetIdentitySeed(this IMutableModel model, long? seed)
        => model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentitySeed,
            seed);

    public static long GetIdentitySeed(this IReadOnlyModel model)
    {
        if (model is RuntimeModel)
        {
            throw new InvalidOperationException(CoreStrings.RuntimeModelMissingData);
        }

        var annotation = model.FindAnnotation(SnowflakeAnnotationNames.IdentitySeed);
        return annotation is null || annotation.Value is null
            ? 1
            : annotation.Value is int intValue
                ? intValue
                : (long)annotation.Value;
    }

    public static ConfigurationSource? GetIdentitySeedConfigurationSource(this IConventionModel model)
        => model.FindAnnotation(SnowflakeAnnotationNames.IdentitySeed)?.GetConfigurationSource();

    public static void SetIdentityIncrement(this IMutableModel model, int? increment)
        => model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentityIncrement,
            increment);

    public static void SetIdentityIsOrdered(this IMutableModel model, bool? ordered)
        => model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentityIsOrdered,
            ordered);

    public static bool? SetIdentityIsOrdered(
        this IConventionModel model,
        bool? ordered,
        bool fromDataAnnotation = false)
        => (bool?)model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentityIsOrdered,
            ordered,
            fromDataAnnotation)?.Value;

    public static bool GetIdentityIsOrdered(this IReadOnlyModel model)
    {
        return (model is RuntimeModel)
            ? throw new InvalidOperationException(CoreStrings.RuntimeModelMissingData)
            : (bool?)model[SnowflakeAnnotationNames.IdentityIsOrdered] ?? true;
    }

    public static ConfigurationSource? GetIdentityIsOrderedConfigurationSource(this IConventionModel model)
        => model.FindAnnotation(SnowflakeAnnotationNames.IdentityIsOrdered)?.GetConfigurationSource();

    public static int? SetIdentityIncrement(
        this IConventionModel model,
        int? increment,
        bool fromDataAnnotation = false)
        => (int?)model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentityIncrement,
            increment,
            fromDataAnnotation)?.Value;

    public static int GetIdentityIncrement(this IReadOnlyModel model)
    {
        return (model is RuntimeModel)
            ? throw new InvalidOperationException(CoreStrings.RuntimeModelMissingData)
            : (int?)model[SnowflakeAnnotationNames.IdentityIncrement] ?? 1;
    }

    public static ConfigurationSource? GetIdentityIncrementConfigurationSource(this IConventionModel model)
        => model.FindAnnotation(SnowflakeAnnotationNames.IdentityIncrement)?.GetConfigurationSource();

    public static SnowflakeIndexBehavior? SetIndexBehavior(
        this IConventionModel model,
        SnowflakeIndexBehavior? indexBehavior,
        bool fromDataAnnotation = false)
        => (SnowflakeIndexBehavior?)model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IndexBehavior,
            indexBehavior,
            fromDataAnnotation)?.Value;

    public static ConfigurationSource? GetIndexBehaviorConfigurationSource(this IConventionModel model)
        => model.FindAnnotation(SnowflakeAnnotationNames.IndexBehavior)?.GetConfigurationSource();

    public static string? GetSequenceSchema(this IReadOnlyModel model)
        => (string?)model[SnowflakeAnnotationNames.SequenceSchema];

    public static void SetSequenceSchema(this IMutableModel model, string? value)
    {
        Check.NullButNotEmpty(value, nameof(value));

        model.SetOrRemoveAnnotation(SnowflakeAnnotationNames.SequenceSchema, value);
    }

    public static string? SetSequenceSchema(
        this IConventionModel model,
        string? value,
        bool fromDataAnnotation = false)
        => (string?)model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.SequenceSchema,
            Check.NullButNotEmpty(value, nameof(value)),
            fromDataAnnotation)?.Value;

    public static ConfigurationSource? GetSequenceSchemaConfigurationSource(this IConventionModel model)
        => model.FindAnnotation(SnowflakeAnnotationNames.SequenceSchema)?.GetConfigurationSource();
}

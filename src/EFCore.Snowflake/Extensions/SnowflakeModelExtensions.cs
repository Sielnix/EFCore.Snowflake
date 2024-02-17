using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.Snowflake.Extensions;
public static class SnowflakeModelExtensions
{
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

    public static long? SetIdentitySeed(this IConventionModel model, long? seed, bool fromDataAnnotation = false)
        => (long?)model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IdentitySeed,
            seed,
            fromDataAnnotation)?.Value;

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

    public static SnowflakeIndexBehavior? SetIndexBehavior(
        this IConventionModel model,
        SnowflakeIndexBehavior? indexBehavior,
        bool fromDataAnnotation = false)
        => (SnowflakeIndexBehavior?)model.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.IndexBehavior,
            indexBehavior,
            fromDataAnnotation)?.Value;
}

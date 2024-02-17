using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EFCore.Snowflake.Extensions;

public static class SnowflakeModelBuilderExtensions
{
    public static IConventionModelBuilder? HasValueGenerationStrategy(
        this IConventionModelBuilder modelBuilder,
        SnowflakeValueGenerationStrategy? valueGenerationStrategy,
        bool fromDataAnnotation = false)
    {
        if (modelBuilder.CanSetValueGenerationStrategy(valueGenerationStrategy, fromDataAnnotation))
        {
            modelBuilder.Metadata.SetValueGenerationStrategy(valueGenerationStrategy, fromDataAnnotation);
            if (valueGenerationStrategy != SnowflakeValueGenerationStrategy.AutoIncrement)
            {
                modelBuilder.HasIdentityColumnSeed(null, fromDataAnnotation);
                modelBuilder.HasIdentityColumnIncrement(null, fromDataAnnotation);
            }

            return modelBuilder;
        }

        return null;
    }

    public static bool CanSetValueGenerationStrategy(
        this IConventionModelBuilder modelBuilder,
        SnowflakeValueGenerationStrategy? valueGenerationStrategy,
        bool fromDataAnnotation = false)
    {
        return modelBuilder.CanSetAnnotation(
            SnowflakeAnnotationNames.ValueGenerationStrategy, valueGenerationStrategy, fromDataAnnotation);
    }

    public static IConventionModelBuilder? HasIdentityColumnSeed(
        this IConventionModelBuilder modelBuilder,
        long? seed,
        bool fromDataAnnotation = false)
    {
        if (modelBuilder.CanSetIdentityColumnSeed(seed, fromDataAnnotation))
        {
            modelBuilder.Metadata.SetIdentitySeed(seed, fromDataAnnotation);
            return modelBuilder;
        }

        return null;
    }

    public static bool CanSetIdentityColumnSeed(
        this IConventionModelBuilder modelBuilder,
        long? seed,
        bool fromDataAnnotation = false)
        => modelBuilder.CanSetAnnotation(SnowflakeAnnotationNames.IdentitySeed, seed, fromDataAnnotation);

    public static IConventionModelBuilder? HasIdentityColumnIncrement(
        this IConventionModelBuilder modelBuilder,
        int? increment,
        bool fromDataAnnotation = false)
    {
        if (modelBuilder.CanSetIdentityColumnIncrement(increment, fromDataAnnotation))
        {
            modelBuilder.Metadata.SetIdentityIncrement(increment, fromDataAnnotation);
            return modelBuilder;
        }

        return null;
    }

    public static bool CanSetIdentityColumnIncrement(
        this IConventionModelBuilder modelBuilder,
        int? increment,
        bool fromDataAnnotation = false)
        => modelBuilder.CanSetAnnotation(SnowflakeAnnotationNames.IdentityIncrement, increment, fromDataAnnotation);

    public static IConventionModelBuilder? HasIndexBehavior(
        this IConventionModelBuilder modelBuilder,
        SnowflakeIndexBehavior? indexBehavior,
        bool fromDataAnnotation = false)
    {
        if (modelBuilder.CanSetIndexBehavior(indexBehavior, fromDataAnnotation))
        {
            modelBuilder.Metadata.SetIndexBehavior(indexBehavior, fromDataAnnotation);

            return modelBuilder;
        }

        return null;
    }

    public static bool CanSetIndexBehavior(
        this IConventionModelBuilder modelBuilder,
        SnowflakeIndexBehavior? indexBehavior,
        bool fromDataAnnotation = false)
    {
        return modelBuilder.CanSetAnnotation(
            SnowflakeAnnotationNames.IndexBehavior, indexBehavior, fromDataAnnotation);
    }


    public static ModelBuilder SetIndexBehavior(
        this ModelBuilder modelBuilder,
        SnowflakeIndexBehavior? value)
    {
        Check.NotNull(modelBuilder, nameof(modelBuilder));

        modelBuilder.Model.SetIndexBehavior(value);

        return modelBuilder;
    }
}

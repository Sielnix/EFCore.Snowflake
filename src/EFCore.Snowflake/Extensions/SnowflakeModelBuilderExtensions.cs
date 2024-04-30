using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

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

            if (valueGenerationStrategy != SnowflakeValueGenerationStrategy.Sequence)
            {
                RemoveKeySequenceAnnotations();
            }

            return modelBuilder;
        }

        return null;

        void RemoveKeySequenceAnnotations()
        {
            if (modelBuilder.CanSetAnnotation(SnowflakeAnnotationNames.SequenceNameSuffix, null)
                && modelBuilder.CanSetAnnotation(SnowflakeAnnotationNames.SequenceSchema, null))
            {
                modelBuilder.Metadata.SetSequenceNameSuffix(null, fromDataAnnotation);
                modelBuilder.Metadata.SetSequenceSchema(null, fromDataAnnotation);
            }
        }
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

    /// <summary>
    ///     Configures the model to use the Snowflake IDENTITY feature to generate values for key properties
    ///     marked as <see cref="ValueGenerated.OnAdd" />, when targeting Snowflake. This is the default
    ///     behavior when targeting Snowflake.
    /// </summary>
    /// <remarks>
    ///     See <see href="https://aka.ms/efcore-docs-modeling">Modeling entity types and relationships</see>
    ///     for more information and examples.
    /// </remarks>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="seed">The value that is used for the very first row loaded into the table.</param>
    /// <param name="increment">The incremental value that is added to the identity value of the previous row that was loaded.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static ModelBuilder UseIdentityColumns(
        this ModelBuilder modelBuilder,
        long seed = 1,
        int increment = 1)
    {
        var model = modelBuilder.Model;

        model.SetValueGenerationStrategy(SnowflakeValueGenerationStrategy.AutoIncrement);
        model.SetIdentitySeed(seed);
        model.SetIdentityIncrement(increment);
        model.SetSequenceSchema(null);

        return modelBuilder;
    }

    /// <summary>
    ///     Configures the model to use a sequence per hierarchy to generate values for key properties
    ///     marked as <see cref="ValueGenerated.OnAdd" />, when targeting SQL Server.
    /// </summary>
    /// <remarks>
    ///     See <see href="https://aka.ms/efcore-docs-modeling">Modeling entity types and relationships</see>, and
    ///     <see href="https://aka.ms/efcore-docs-sqlserver">Accessing SQL Server and Azure SQL databases with EF Core</see>
    ///     for more information and examples.
    /// </remarks>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="nameSuffix">The name that will suffix the table name for each sequence created automatically.</param>
    /// <param name="schema">The schema of the sequence.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static ModelBuilder UseKeySequences(
        this ModelBuilder modelBuilder,
        string? nameSuffix = null,
        string? schema = null)
    {
        Check.NullButNotEmpty(nameSuffix, nameof(nameSuffix));
        Check.NullButNotEmpty(schema, nameof(schema));

        var model = modelBuilder.Model;

        nameSuffix ??= SnowflakeModelExtensions.DefaultSequenceNameSuffix;

        model.SetValueGenerationStrategy(SnowflakeValueGenerationStrategy.Sequence);
        model.SetSequenceNameSuffix(nameSuffix);
        model.SetSequenceSchema(schema);
        model.SetIdentitySeed(null);
        model.SetIdentityIncrement(null);

        return modelBuilder;
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

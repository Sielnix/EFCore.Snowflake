using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System.Reflection;

namespace EFCore.Snowflake.Design.Internal;

public class SnowflakeAnnotationCodeGenerator(AnnotationCodeGeneratorDependencies dependencies)
    : AnnotationCodeGenerator(dependencies)
{
    private static readonly MethodInfo ModelUseIdentityColumnsMethodInfo
        = typeof(SnowflakeModelBuilderExtensions).GetRuntimeMethod(
            nameof(SnowflakeModelBuilderExtensions.UseIdentityColumns), [typeof(ModelBuilder), typeof(long), typeof(int), typeof(bool)])!;

    private static readonly MethodInfo ModelUseKeySequencesMethodInfo
        = typeof(SnowflakeModelBuilderExtensions).GetRuntimeMethod(
            nameof(SnowflakeModelBuilderExtensions.UseKeySequences), [typeof(ModelBuilder), typeof(string), typeof(string)])!;

    private static readonly MethodInfo ModelHasAnnotationMethodInfo
        = typeof(ModelBuilder).GetRuntimeMethod(
            nameof(ModelBuilder.HasAnnotation), [typeof(string), typeof(object)])!;

    private static readonly MethodInfo PropertyUseIdentityColumnsMethodInfo
        = typeof(SnowflakePropertyBuilderExtensions).GetRuntimeMethod(
            nameof(SnowflakePropertyBuilderExtensions.UseIdentityColumn), [typeof(PropertyBuilder), typeof(long), typeof(int), typeof(bool)])!;

    private static readonly MethodInfo PropertyUseSequenceMethodInfo
        = typeof(SnowflakePropertyBuilderExtensions).GetRuntimeMethod(
            nameof(SnowflakePropertyBuilderExtensions.UseSequence), [typeof(PropertyBuilder), typeof(string), typeof(string)])!;

    private static readonly MethodInfo SequenceIsOrderedMethodInfo
        = typeof(SnowflakeSequenceBuilderExtensions).GetRuntimeMethod(
            nameof(SnowflakeSequenceBuilderExtensions.IsOrdered), [typeof(SequenceBuilder), typeof(bool)])!;

    protected override bool IsHandledByConvention(IModel model, IAnnotation annotation)
    {
        if (annotation.Name == SnowflakeAnnotationNames.ValueGenerationStrategy)
        {
            return (SnowflakeValueGenerationStrategy)annotation.Value! == SnowflakeValueGenerationStrategy.AutoIncrement;
        }

        if (annotation.Name == SnowflakeAnnotationNames.IndexBehavior)
        {
            return (SnowflakeIndexBehavior)annotation.Value! == SnowflakeIndexBehavior.Disallow;
        }

        return base.IsHandledByConvention(model, annotation);
    }
    
    protected override bool IsHandledByConvention(ISequence sequence, IAnnotation annotation)
    {
        if (annotation.Name == SnowflakeAnnotationNames.SequenceIsOrdered)
        {
            bool ordered = (bool)annotation.Value!;
            return ordered;
        }

        return base.IsHandledByConvention(sequence, annotation);
    }

    public override IReadOnlyList<MethodCallCodeFragment> GenerateFluentApiCalls(IModel model, IDictionary<string, IAnnotation> annotations)
    {
        List<MethodCallCodeFragment> fragments = new(base.GenerateFluentApiCalls(model, annotations));

        if (GenerateValueGenerationStrategy(annotations, model, onModel: true) is { } valueGenerationStrategy)
        {
            fragments.Add(valueGenerationStrategy);
        }

        return fragments;
    }

    public override IReadOnlyList<MethodCallCodeFragment> GenerateFluentApiCalls(IProperty property, IDictionary<string, IAnnotation> annotations)
    {
        List<MethodCallCodeFragment> fragments = new(base.GenerateFluentApiCalls(property, annotations));

        if (GenerateValueGenerationStrategy(annotations, property.DeclaringType.Model, onModel: false) is { } valueGenerationStrategy)
        {
            fragments.Add(valueGenerationStrategy);
        }

        return fragments;
    }

    protected override MethodCallCodeFragment? GenerateFluentApi(ISequence sequence, IAnnotation annotation)
    {
        if (annotation.Name == SnowflakeAnnotationNames.SequenceIsOrdered)
        {
            bool isOrdered = (bool)annotation.Value!;

            return new MethodCallCodeFragment(SequenceIsOrderedMethodInfo, isOrdered ? [] : [isOrdered]);
        }

        return base.GenerateFluentApi(sequence, annotation);
    }

    private static MethodCallCodeFragment? GenerateValueGenerationStrategy(
        IDictionary<string, IAnnotation> annotations,
        IModel model,
        bool onModel)
    {
        SnowflakeValueGenerationStrategy strategy;
        if (annotations.TryGetValue(SnowflakeAnnotationNames.ValueGenerationStrategy, out var strategyAnnotation)
            && strategyAnnotation.Value != null)
        {
            annotations.Remove(SnowflakeAnnotationNames.ValueGenerationStrategy);
            strategy = (SnowflakeValueGenerationStrategy)strategyAnnotation.Value;
        }
        else
        {
            return null;
        }

        switch (strategy)
        {
            case SnowflakeValueGenerationStrategy.AutoIncrement:
                if (annotations.TryGetValue(SnowflakeAnnotationNames.IdentitySeed, out var seedAnnotation)
                    && seedAnnotation.Value != null)
                {
                    annotations.Remove(SnowflakeAnnotationNames.IdentitySeed);
                }
                else
                {
                    seedAnnotation = model.FindAnnotation(SnowflakeAnnotationNames.IdentitySeed);
                }

                long seed = seedAnnotation is null
                    ? 1L
                    : seedAnnotation.Value is int intValue
                        ? intValue
                        : (long?)seedAnnotation.Value ?? 1L;

                var increment = GetAndRemove<int?>(annotations, SnowflakeAnnotationNames.IdentityIncrement)
                    ?? model.FindAnnotation(SnowflakeAnnotationNames.IdentityIncrement)?.Value as int?
                    ?? 1;

                bool ordered = GetAndRemove<bool?>(annotations, SnowflakeAnnotationNames.IdentityIsOrdered)
                                ?? model.FindAnnotation(SnowflakeAnnotationNames.IdentityIsOrdered)?.Value as bool?
                                ?? true;

                return new MethodCallCodeFragment(
                    onModel ? ModelUseIdentityColumnsMethodInfo : PropertyUseIdentityColumnsMethodInfo,
                    (seed, increment, ordered) switch
                    {
                        (1L, 1, true) => Array.Empty<object>(),
                        (_, 1, true) => new object[] { seed },
                        (_, _, true) => new object[] { seed, increment },
                        _ => new object[] { seed, increment, ordered }
                    });

            case SnowflakeValueGenerationStrategy.Sequence:
                {
                    var nameOrSuffix = GetAndRemove<string>(
                        annotations,
                        onModel ? SnowflakeAnnotationNames.SequenceNameSuffix : SnowflakeAnnotationNames.SequenceName);

                    var schema = GetAndRemove<string>(annotations, SnowflakeAnnotationNames.SequenceSchema);
                    return new MethodCallCodeFragment(
                        onModel ? ModelUseKeySequencesMethodInfo : PropertyUseSequenceMethodInfo,
                        (name: nameOrSuffix, schema) switch
                        {
                            (null, null) => Array.Empty<object>(),
                            (_, null) => new object[] { nameOrSuffix },
                            _ => new object[] { nameOrSuffix!, schema }
                        });
                }

            case SnowflakeValueGenerationStrategy.None:
                return new MethodCallCodeFragment(
                    ModelHasAnnotationMethodInfo,
                    SnowflakeAnnotationNames.ValueGenerationStrategy,
                    SnowflakeValueGenerationStrategy.None);

            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private static T? GetAndRemove<T>(IDictionary<string, IAnnotation> annotations, string annotationName)
    {
        if (annotations.TryGetValue(annotationName, out var annotation)
            && annotation.Value != null)
        {
            annotations.Remove(annotationName);
            return (T)annotation.Value;
        }

        return default;
    }
}

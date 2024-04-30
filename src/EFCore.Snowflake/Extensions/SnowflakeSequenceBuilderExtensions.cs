using EFCore.Snowflake.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

public static class SnowflakeSequenceBuilderExtensions
{
    public static SequenceBuilder IsOrdered(this SequenceBuilder sequenceBuilder, bool ordered = true)
    {
        sequenceBuilder.Metadata.SetIsOrdered(ordered);

        return sequenceBuilder;
    }

    public static IConventionSequenceBuilder IsOrdered(
        this IConventionSequenceBuilder sequenceBuilder,
        bool ordered,
        bool fromDataAnnotation = false)
    {
        if (sequenceBuilder.CanSetIsOrdered(ordered, fromDataAnnotation))
        {
            sequenceBuilder.Metadata.SetIsOrdered(ordered, fromDataAnnotation);
        }

        return sequenceBuilder;
    }

    public static bool CanSetIsOrdered(
        this IConventionSequenceBuilder sequenceBuilder,
        bool ordered,
        bool fromDataAnnotation = false)
    {

        return sequenceBuilder.CanSetAnnotation(
            SnowflakeAnnotationNames.SequenceIsOrdered,
            ordered,
            fromDataAnnotation);
    }
}

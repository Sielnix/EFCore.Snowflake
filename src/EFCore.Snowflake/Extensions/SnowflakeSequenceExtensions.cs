using EFCore.Snowflake.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore;

public static class SnowflakeSequenceExtensions
{
    public static void SetIsOrdered(this IMutableSequence sequence, bool ordered)
    {
        sequence.SetOrRemoveAnnotation(SnowflakeAnnotationNames.SequenceIsOrdered, ordered);
    }

    public static bool? SetIsOrdered(
        this IConventionSequence sequence,
        bool ordered,
        bool fromDataAnnotation = false)
        => (bool?)sequence.SetOrRemoveAnnotation(
            SnowflakeAnnotationNames.SequenceIsOrdered,
            ordered,
            fromDataAnnotation)?.Value;
}

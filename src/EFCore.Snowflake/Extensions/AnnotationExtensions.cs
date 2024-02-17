using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EFCore.Snowflake.Extensions;
internal static class AnnotationExtensions
{
    public static bool HasStringValueSet(this IAnnotation? annotation)
    {
        return annotation != null && annotation.Value != null && !string.IsNullOrEmpty(annotation.Value.ToString());
    }
}

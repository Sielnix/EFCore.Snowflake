using EFCore.Snowflake.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EFCore.Snowflake.Metadata.Conventions;

/// <summary>
///     A convention that creates an optimized copy of the mutable model.
/// </summary>
/// <remarks>
///     See <see href="https://aka.ms/efcore-docs-conventions">Model building conventions</see>, and
/// </remarks>
public class SnowflakeRuntimeModelConvention(
    ProviderConventionSetBuilderDependencies dependencies,
    RelationalConventionSetBuilderDependencies relationalDependencies)
    : RelationalRuntimeModelConvention(dependencies, relationalDependencies)
{
    /// <inheritdoc />
    protected override void ProcessPropertyAnnotations(
        Dictionary<string, object?> annotations,
        IProperty property,
        RuntimeProperty runtimeProperty,
        bool runtime)
    {
        base.ProcessPropertyAnnotations(annotations, property, runtimeProperty, runtime);

        if (!runtime)
        {
            annotations.Remove(SnowflakeAnnotationNames.IdentityIncrement);
            annotations.Remove(SnowflakeAnnotationNames.IdentitySeed);
            annotations.Remove(SnowflakeAnnotationNames.IdentityIsOrdered);

            if (!annotations.ContainsKey(SnowflakeAnnotationNames.ValueGenerationStrategy))
            {
                annotations[SnowflakeAnnotationNames.ValueGenerationStrategy] = property.GetValueGenerationStrategy();
            }
        }
    }

    protected override void ProcessEntityTypeAnnotations(
        Dictionary<string, object?> annotations,
        IEntityType entityType,
        RuntimeEntityType runtimeEntityType,
        bool runtime)
    {
        base.ProcessEntityTypeAnnotations(annotations, entityType, runtimeEntityType, runtime);

        if (!runtime)
        {
            annotations.Remove(SnowflakeAnnotationNames.TableType);
        }
    }
}

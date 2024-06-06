using EFCore.Snowflake.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.Snowflake.Design.Internal;

internal class SnowflakeCSharpRuntimeAnnotationCodeGenerator(
    CSharpRuntimeAnnotationCodeGeneratorDependencies dependencies,
    RelationalCSharpRuntimeAnnotationCodeGeneratorDependencies relationalDependencies)
    : RelationalCSharpRuntimeAnnotationCodeGenerator(dependencies, relationalDependencies)
{
    public override void Generate(IModel model, CSharpRuntimeAnnotationCodeGeneratorParameters parameters)
    {
        if (!parameters.IsRuntime)
        {
            var annotations = parameters.Annotations;
            annotations.Remove(SnowflakeAnnotationNames.IdentityIncrement);
            annotations.Remove(SnowflakeAnnotationNames.IdentitySeed);
            annotations.Remove(SnowflakeAnnotationNames.IdentityIsOrdered);
        }

        base.Generate(model, parameters);
    }

    public override void Generate(IProperty property, CSharpRuntimeAnnotationCodeGeneratorParameters parameters)
    {
        if (!parameters.IsRuntime)
        {
            var annotations = parameters.Annotations;
            annotations.Remove(SnowflakeAnnotationNames.IdentityIncrement);
            annotations.Remove(SnowflakeAnnotationNames.IdentitySeed);
            annotations.Remove(SnowflakeAnnotationNames.IdentityIsOrdered);

            if (!annotations.ContainsKey(SnowflakeAnnotationNames.ValueGenerationStrategy))
            {
                annotations[SnowflakeAnnotationNames.ValueGenerationStrategy] = property.GetValueGenerationStrategy();
            }
        }

        base.Generate(property, parameters);
    }

    public override void Generate(IColumn column, CSharpRuntimeAnnotationCodeGeneratorParameters parameters)
    {
        if (!parameters.IsRuntime)
        {
            var annotations = parameters.Annotations;
            annotations.Remove(SnowflakeAnnotationNames.Identity);
        }

        base.Generate(column, parameters);
    }

    public override void Generate(IRelationalPropertyOverrides overrides, CSharpRuntimeAnnotationCodeGeneratorParameters parameters)
    {
        if (!parameters.IsRuntime)
        {
            var annotations = parameters.Annotations;
            annotations.Remove(SnowflakeAnnotationNames.IdentityIncrement);
            annotations.Remove(SnowflakeAnnotationNames.IdentitySeed);
            annotations.Remove(SnowflakeAnnotationNames.IdentityIsOrdered);
        }

        base.Generate(overrides, parameters);
    }
}

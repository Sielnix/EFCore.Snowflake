using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.Snowflake.Design.Internal;

public class SnowflakeAnnotationCodeGenerator(AnnotationCodeGeneratorDependencies dependencies)
    : AnnotationCodeGenerator(dependencies)
{
    public override IReadOnlyList<MethodCallCodeFragment> GenerateFluentApiCalls(IProperty property, IDictionary<string, IAnnotation> annotations)
    {
        return base.GenerateFluentApiCalls(property, annotations);
    }
}

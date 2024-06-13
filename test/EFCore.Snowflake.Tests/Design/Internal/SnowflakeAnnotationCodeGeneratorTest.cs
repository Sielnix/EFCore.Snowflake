using EFCore.Snowflake.Design.Internal;
using EFCore.Snowflake.Metadata.Conventions;
using EFCore.Snowflake.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Tests.Design.Internal;

public class SnowflakeAnnotationCodeGeneratorTest
{
    [ConditionalFact]
    public void GenerateFluentApi_sequence_works_when_not_ordered()
    {
        var generator = CreateGenerator();

        var modelBuilder = SnowflakeConventionSetBuilder.CreateModelBuilder();
        modelBuilder.HasSequence("TestSeq").IsOrdered(false);
        
        ISequence sequence = modelBuilder.FinalizeModel().GetSequences().Single();
        
        var result = generator.GenerateFluentApiCalls(sequence, sequence.GetAnnotations().ToDictionary(a => a.Name, a => a))
            .Single();

        Assert.Equal("IsOrdered", result.Method);

        object? argument = Assert.Single(result.Arguments);
        Assert.Equal(false, argument);
    }

    private SnowflakeAnnotationCodeGenerator CreateGenerator()
        => new(
            new AnnotationCodeGeneratorDependencies(
                new SnowflakeTypeMappingSource(
                    new TypeMappingSourceDependencies(
                        new ValueConverterSelector(
                            new ValueConverterSelectorDependencies()),
                        new JsonValueReaderWriterSource(new JsonValueReaderWriterSourceDependencies()),
                        Array.Empty<ITypeMappingSourcePlugin>()),
                    new RelationalTypeMappingSourceDependencies(
                        Array.Empty<IRelationalTypeMappingSourcePlugin>()))));
}

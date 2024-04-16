using EFCore.Snowflake.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Metadata.Conventions;

public class SnowflakeValueGenerationConvention(
    ProviderConventionSetBuilderDependencies dependencies,
    RelationalConventionSetBuilderDependencies relationalDependencies)
    : RelationalValueGenerationConvention(dependencies, relationalDependencies)
{
    /// <inheritdoc />
    public override void ProcessPropertyAnnotationChanged(
        IConventionPropertyBuilder propertyBuilder,
        string name,
        IConventionAnnotation? annotation,
        IConventionAnnotation? oldAnnotation,
        IConventionContext<IConventionAnnotation> context)
    {
        if (name == SnowflakeAnnotationNames.ValueGenerationStrategy)
        {
            propertyBuilder.ValueGenerated(GetValueGenerated(propertyBuilder.Metadata));
            return;
        }

        base.ProcessPropertyAnnotationChanged(propertyBuilder, name, annotation, oldAnnotation, context);
    }

    protected override ValueGenerated? GetValueGenerated(IConventionProperty property)
    {
        // TODO: move to relational?
        if (property.DeclaringType.IsMappedToJson()
#pragma warning disable EF1001 // Internal EF Core API usage.
            && property.IsOrdinalKeyProperty()
#pragma warning restore EF1001 // Internal EF Core API usage.
            && (property.DeclaringType as IReadOnlyEntityType)?.FindOwnership()!.IsUnique == false)
        {
            return ValueGenerated.OnAdd;
        }

        var declaringTable = property.GetMappedStoreObjects(StoreObjectType.Table).FirstOrDefault();
        if (declaringTable.Name == null)
        {
            return null;
        }

        // If the first mapping can be value generated then we'll consider all mappings to be value generated
        // as this is a client-side configuration and can't be specified per-table.
        return GetValueGenerated(property, declaringTable, Dependencies.TypeMappingSource);
    }

    private static ValueGenerated? GetValueGenerated(
        IReadOnlyProperty property,
        in StoreObjectIdentifier storeObject,
        ITypeMappingSource typeMappingSource)
        => RelationalValueGenerationConvention.GetValueGenerated(property, storeObject)
           ?? (property.GetValueGenerationStrategy(storeObject, typeMappingSource) != SnowflakeValueGenerationStrategy.None
               ? ValueGenerated.OnAdd
               : null);
}

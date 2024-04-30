using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EFCore.Snowflake.Metadata.Conventions;

public class SnowflakeValueGenerationStrategyConvention(
    ProviderConventionSetBuilderDependencies dependencies,
    RelationalConventionSetBuilderDependencies relationalDependencies)
    : IModelInitializedConvention, IModelFinalizingConvention
{
    protected virtual ProviderConventionSetBuilderDependencies Dependencies { get; } = dependencies;

    protected virtual RelationalConventionSetBuilderDependencies RelationalDependencies { get; } = relationalDependencies;

    public void ProcessModelInitialized(IConventionModelBuilder modelBuilder, IConventionContext<IConventionModelBuilder> context)
    {
         modelBuilder.HasValueGenerationStrategy(SnowflakeValueGenerationStrategy.AutoIncrement);
    }

    public virtual void ProcessModelFinalizing(
            IConventionModelBuilder modelBuilder,
            IConventionContext<IConventionModelBuilder> context)
    {
        foreach (var entityType in modelBuilder.Metadata.GetEntityTypes())
        {
            foreach (var property in entityType.GetDeclaredProperties())
            {
                SnowflakeValueGenerationStrategy? strategy = null;
                var declaringTable = property.GetMappedStoreObjects(StoreObjectType.Table).FirstOrDefault();
                if (declaringTable.Name != null!)
                {
                    strategy = property.GetValueGenerationStrategy(declaringTable, Dependencies.TypeMappingSource);
                    if (strategy == SnowflakeValueGenerationStrategy.None
                        && !IsStrategyNoneNeeded(property, declaringTable))
                    {
                        strategy = null;
                    }
                }
                else
                {
                    var declaringView = property.GetMappedStoreObjects(StoreObjectType.View).FirstOrDefault();
                    if (declaringView.Name != null!)
                    {
                        strategy = property.GetValueGenerationStrategy(declaringView, Dependencies.TypeMappingSource);
                        if (strategy == SnowflakeValueGenerationStrategy.None
                            && !IsStrategyNoneNeeded(property, declaringView))
                        {
                            strategy = null;
                        }
                    }
                }

                // Needed for the annotation to show up in the model snapshot
                if (strategy != null
                    && declaringTable.Name != null)
                {
                    property.Builder.HasValueGenerationStrategy(strategy);

                    if (strategy == SnowflakeValueGenerationStrategy.Sequence)
                    {
                        var sequence = modelBuilder.HasSequence(
                            property.GetSequenceName(declaringTable)
                            ?? entityType.GetRootType().ShortName() + modelBuilder.Metadata.GetSequenceNameSuffix(),
                            property.GetSequenceSchema(declaringTable)
                            ?? modelBuilder.Metadata.GetSequenceSchema()).Metadata;

                        property.Builder.HasDefaultValueSql(
                            RelationalDependencies.UpdateSqlGenerator.GenerateObtainNextSequenceValueOperation(
                                sequence.Name, sequence.Schema));
                    }
                }
            }
        }

        bool IsStrategyNoneNeeded(IReadOnlyProperty property, StoreObjectIdentifier storeObject)
        {
            if (property.ValueGenerated == ValueGenerated.OnAdd
                && !property.TryGetDefaultValue(storeObject, out _)
                && property.GetDefaultValueSql(storeObject) == null
                && property.GetComputedColumnSql(storeObject) == null
                && property.DeclaringType.Model.GetValueGenerationStrategy() == SnowflakeValueGenerationStrategy.AutoIncrement)
            {
                var providerClrType = (property.GetValueConverter()
                        ?? (property.FindRelationalTypeMapping(storeObject)
                            ?? Dependencies.TypeMappingSource.FindMapping((IProperty)property))?.Converter)
                    ?.ProviderClrType.UnwrapNullableType();

                return providerClrType != null
                    && (providerClrType.IsInteger() || providerClrType == typeof(decimal));
            }

            return false;
        }
    }
}

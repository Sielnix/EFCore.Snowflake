using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.ModelBuilding;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Associations;
using Microsoft.EntityFrameworkCore.Query.Associations.ComplexJson;
using Microsoft.EntityFrameworkCore.Query.Associations.ComplexProperties;
using Microsoft.EntityFrameworkCore.Query.Associations.ComplexTableSplitting;
using Microsoft.EntityFrameworkCore.Query.Associations.Navigations;
using Microsoft.EntityFrameworkCore.Query.Associations.OwnedJson;
using Microsoft.EntityFrameworkCore.Query.Associations.OwnedNavigations;
using Microsoft.EntityFrameworkCore.Query.Associations.OwnedTableSplitting;
using Microsoft.EntityFrameworkCore.Query.Translations;
using Microsoft.EntityFrameworkCore.Query.Translations.Operators;
using Microsoft.EntityFrameworkCore.Query.Translations.Temporal;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Types;
using Microsoft.EntityFrameworkCore.Update;

namespace EFCore.Snowflake.FunctionalTests;

public class SnowflakeComplianceTest : RelationalComplianceTestBase
{
    protected override ICollection<Type> IgnoredTestBases { get; } =
    [
        typeof(AdHocManyToManyQueryTestBase),
        typeof(AdHocMiscellaneousQueryTestBase),
        typeof(BadDataJsonDeserializationTestBase),
        typeof(EntityFrameworkServiceCollectionExtensionsTestBase),
        typeof(GraphUpdatesTestBase<>),
        typeof(JsonTypesRelationalTestBase),
        typeof(JsonTypesTestBase),
        typeof(LazyLoadProxyRelationalTestBase<>),
        typeof(OptimisticConcurrencyRelationalTestBase<,>),
        typeof(OptimisticConcurrencyTestBase<,>),
        typeof(OverzealousInitializationTestBase<>),
        typeof(OwnedEntityQueryTestBase),
        typeof(PropertyValuesRelationalTestBase<>),
        typeof(PropertyValuesTestBase<>),
        typeof(ProxyGraphUpdatesTestBase<>),
        typeof(QueryExpressionInterceptionTestBase),
        typeof(RelationalServiceCollectionExtensionsTestBase),
        typeof(SaveChangesInterceptionTestBase),
        typeof(SeedingTestBase),
        typeof(SerializationTestBase<>),
        typeof(SharedTypeQueryTestBase),
        typeof(SpatialTestBase<>),
        typeof(StoreGeneratedFixupRelationalTestBase<>),
        typeof(StoreGeneratedFixupTestBase<>),
        typeof(StoreGeneratedTestBase<>),
        typeof(TPTTableSplittingTestBase),
        typeof(TableSplittingTestBase),
        typeof(TransactionInterceptionTestBase),
        typeof(TransactionTestBase<>),
        typeof(TwoDatabasesTestBase),
        typeof(ValueConvertersEndToEndTestBase<>),
        typeof(WithConstructorsTestBase<>),

        // BulkUpdates
        typeof(BulkUpdatesTestBase<>),
        typeof(FiltersInheritanceBulkUpdatesRelationalTestBase<>),
        typeof(FiltersInheritanceBulkUpdatesTestBase<>),
        typeof(InheritanceBulkUpdatesRelationalTestBase<>),
        typeof(InheritanceBulkUpdatesTestBase<>),
        typeof(NonSharedModelBulkUpdatesRelationalTestBase),
        typeof(NonSharedModelBulkUpdatesTestBase),
        typeof(NorthwindBulkUpdatesRelationalTestBase<>),
        typeof(NorthwindBulkUpdatesTestBase<>),
        typeof(TPCFiltersInheritanceBulkUpdatesTestBase<>),
        typeof(TPCInheritanceBulkUpdatesTestBase<>),
        typeof(TPHInheritanceBulkUpdatesTestBase<>),
        typeof(TPTFiltersInheritanceBulkUpdatesTestBase<>),
        typeof(TPTInheritanceBulkUpdatesTestBase<>),

        // Migrations
        typeof(MigrationsSqlGeneratorTestBase),

        // ModelBuilding
        typeof(ModelBuilderTest.ComplexCollectionTestBase),
        typeof(ModelBuilderTest.ComplexTypeTestBase),
        typeof(ModelBuilderTest.InheritanceTestBase),
        typeof(ModelBuilderTest.ManyToManyTestBase),
        typeof(ModelBuilderTest.ManyToOneTestBase),
        typeof(ModelBuilderTest.ModelBuilderTestBase),
        typeof(ModelBuilderTest.NonRelationshipTestBase),
        typeof(ModelBuilderTest.OneToManyTestBase),
        typeof(ModelBuilderTest.OneToOneTestBase),
        typeof(ModelBuilderTest.OwnedTypesTestBase),
        typeof(RelationalModelBuilderTest.RelationalComplexCollectionTestBase),
        typeof(RelationalModelBuilderTest.RelationalComplexTypeTestBase),
        typeof(RelationalModelBuilderTest.RelationalInheritanceTestBase),
        typeof(RelationalModelBuilderTest.RelationalManyToManyTestBase),
        typeof(RelationalModelBuilderTest.RelationalManyToOneTestBase),
        typeof(RelationalModelBuilderTest.RelationalNonRelationshipTestBase),
        typeof(RelationalModelBuilderTest.RelationalOneToManyTestBase),
        typeof(RelationalModelBuilderTest.RelationalOneToOneTestBase),
        typeof(RelationalModelBuilderTest.RelationalOwnedTypesTestBase),

        // Query
        typeof(AdHocAdvancedMappingsQueryRelationalTestBase),
        typeof(AdHocAdvancedMappingsQueryTestBase),
        typeof(AdHocComplexTypeQueryRelationalTestBase),
        typeof(AdHocComplexTypeQueryTestBase),
        typeof(AdHocJsonQueryRelationalTestBase),
        typeof(AdHocJsonQueryTestBase),
        typeof(AdHocManyToManyQueryRelationalTestBase),
        typeof(AdHocMiscellaneousQueryRelationalTestBase),
        typeof(AdHocNavigationsQueryRelationalTestBase),
        typeof(AdHocNavigationsQueryTestBase),
        typeof(AdHocPrecompiledQueryRelationalTestBase),
        typeof(AdHocQueryFiltersQueryRelationalTestBase),
        typeof(AdHocQueryFiltersQueryTestBase),
        typeof(AdHocQuerySplittingQueryTestBase),
        typeof(FilteredQueryTestBase<>),
        typeof(FiltersInheritanceQueryTestBase<>),
        typeof(FromSqlQueryTestBase<>),
        typeof(FromSqlSprocQueryTestBase<>),
        typeof(IncludeOneToOneTestBase<>),
        typeof(InheritanceQueryTestBase<>),
        typeof(InheritanceRelationshipsQueryRelationalTestBase<>),
        typeof(InheritanceRelationshipsQueryTestBase<>),
        typeof(JsonQueryRelationalTestBase<>),
        typeof(JsonQueryTestBase<>),
        typeof(ManyToManyNoTrackingQueryRelationalTestBase<>),
        typeof(ManyToManyNoTrackingQueryTestBase<>),
        typeof(ManyToManyQueryRelationalTestBase<>),
        typeof(ManyToManyQueryTestBase<>),
        typeof(MappingQueryTestBase<>),
        typeof(NonSharedPrimitiveCollectionsQueryRelationalTestBase),
        typeof(NonSharedPrimitiveCollectionsQueryTestBase),
        typeof(NorthwindAggregateOperatorsQueryRelationalTestBase<>),
        typeof(NorthwindAggregateOperatorsQueryTestBase<>),
        typeof(NorthwindAsNoTrackingQueryTestBase<>),
        typeof(NorthwindAsTrackingQueryTestBase<>),
        typeof(NorthwindChangeTrackingQueryTestBase<>),
        typeof(NorthwindCompiledQueryTestBase<>),
        typeof(NorthwindDbFunctionsQueryRelationalTestBase<>),
        typeof(NorthwindDbFunctionsQueryTestBase<>),
        typeof(NorthwindEFPropertyIncludeQueryTestBase<>),
        typeof(NorthwindFunctionsQueryRelationalTestBase<>),
        typeof(NorthwindFunctionsQueryTestBase<>),
        typeof(NorthwindGroupByQueryRelationalTestBase<>),
        typeof(NorthwindGroupByQueryTestBase<>),
        typeof(NorthwindIncludeNoTrackingQueryTestBase<>),
        typeof(NorthwindIncludeQueryRelationalTestBase<>),
        typeof(NorthwindIncludeQueryTestBase<>),
        typeof(NorthwindJoinQueryRelationalTestBase<>),
        typeof(NorthwindJoinQueryTestBase<>),
        typeof(NorthwindKeylessEntitiesQueryRelationalTestBase<>),
        typeof(NorthwindKeylessEntitiesQueryTestBase<>),
        typeof(NorthwindMiscellaneousQueryRelationalTestBase<>),
        typeof(NorthwindMiscellaneousQueryTestBase<>),
        typeof(NorthwindNavigationsQueryRelationalTestBase<>),
        typeof(NorthwindNavigationsQueryTestBase<>),
        typeof(NorthwindQueryFiltersQueryTestBase<>),
        typeof(NorthwindQueryTaggingQueryTestBase<>),
        typeof(NorthwindSelectQueryRelationalTestBase<>),
        typeof(NorthwindSelectQueryTestBase<>),
        typeof(NorthwindSetOperationsQueryRelationalTestBase<>),
        typeof(NorthwindSetOperationsQueryTestBase<>),
        typeof(NorthwindSplitIncludeNoTrackingQueryTestBase<>),
        typeof(NorthwindSplitIncludeQueryTestBase<>),
        typeof(NorthwindSqlQueryTestBase<>),
        typeof(NorthwindStringIncludeQueryTestBase<>),
        typeof(NorthwindWhereQueryRelationalTestBase<>),
        typeof(NorthwindWhereQueryTestBase<>),
        typeof(NullKeysTestBase<>),
        typeof(NullSemanticsQueryTestBase<>),
        typeof(OperatorsProceduralQueryTestBase),
        typeof(OperatorsQueryTestBase),
        typeof(OptionalDependentQueryTestBase<>),
        typeof(OwnedEntityQueryRelationalTestBase),
        typeof(OwnedQueryRelationalTestBase<>),
        typeof(OwnedQueryTestBase<>),
        typeof(PrecompiledQueryRelationalTestBase),
        typeof(PrecompiledSqlPregenerationQueryRelationalTestBase),
        typeof(PrimitiveCollectionsQueryRelationalTestBase<>),
        typeof(PrimitiveCollectionsQueryTestBase<>),
        typeof(QueryFilterFuncletizationTestBase<>),
        typeof(QueryNoClientEvalTestBase<>),
        typeof(SharedTypeQueryRelationalTestBase),
        typeof(SpatialQueryRelationalTestBase<>),
        typeof(SpatialQueryTestBase<>),
        typeof(SqlExecutorTestBase<>),
        typeof(SqlQueryTestBase<>),
        typeof(TPCFiltersInheritanceQueryTestBase<>),
        typeof(TPCGearsOfWarQueryRelationalTestBase<>),
        typeof(TPCInheritanceQueryTestBase<>),
        typeof(TPCManyToManyNoTrackingQueryRelationalTestBase<>),
        typeof(TPCManyToManyQueryRelationalTestBase<>),
        typeof(TPCRelationshipsQueryTestBase<>),
        typeof(TPHInheritanceQueryTestBase<>),
        typeof(TPTFiltersInheritanceQueryTestBase<>),
        typeof(TPTGearsOfWarQueryRelationalTestBase<>),
        typeof(TPTInheritanceQueryTestBase<>),
        typeof(TPTManyToManyNoTrackingQueryRelationalTestBase<>),
        typeof(TPTManyToManyQueryRelationalTestBase<>),
        typeof(TPTRelationshipsQueryTestBase<>),
        typeof(ToSqlQueryTestBase),
        typeof(UdfDbFunctionTestBase<>),
        typeof(WarningsTestBase<>),

        // Query/Associations
        typeof(AssociationsBulkUpdateTestBase<>),
        typeof(AssociationsCollectionTestBase<>),
        typeof(AssociationsMiscellaneousTestBase<>),
        typeof(AssociationsPrimitiveCollectionTestBase<>),
        typeof(AssociationsProjectionTestBase<>),
        typeof(AssociationsSetOperationsTestBase<>),
        typeof(AssociationsStructuralEqualityTestBase<>),

        // Query/Associations/ComplexJson
        typeof(ComplexJsonBulkUpdateRelationalTestBase<>),
        typeof(ComplexJsonCollectionRelationalTestBase<>),
        typeof(ComplexJsonMiscellaneousRelationalTestBase<>),
        typeof(ComplexJsonPrimitiveCollectionRelationalTestBase<>),
        typeof(ComplexJsonProjectionRelationalTestBase<>),
        typeof(ComplexJsonSetOperationsRelationalTestBase<>),
        typeof(ComplexJsonStructuralEqualityRelationalTestBase<>),

        // Query/Associations/ComplexProperties
        typeof(ComplexPropertiesBulkUpdateTestBase<>),
        typeof(ComplexPropertiesCollectionTestBase<>),
        typeof(ComplexPropertiesMiscellaneousTestBase<>),
        typeof(ComplexPropertiesPrimitiveCollectionTestBase<>),
        typeof(ComplexPropertiesProjectionTestBase<>),
        typeof(ComplexPropertiesSetOperationsTestBase<>),
        typeof(ComplexPropertiesStructuralEqualityTestBase<>),

        // Query/Associations/ComplexTableSplitting
        typeof(ComplexTableSplittingBulkUpdateRelationalTestBase<>),
        typeof(ComplexTableSplittingMiscellaneousRelationalTestBase<>),
        typeof(ComplexTableSplittingPrimitiveCollectionRelationalTestBase<>),
        typeof(ComplexTableSplittingProjectionRelationalTestBase<>),
        typeof(ComplexTableSplittingStructuralEqualityRelationalTestBase<>),

        // Query/Associations/Navigations
        typeof(NavigationsCollectionRelationalTestBase<>),
        typeof(NavigationsCollectionTestBase<>),
        typeof(NavigationsIncludeRelationalTestBase<>),
        typeof(NavigationsIncludeTestBase<>),
        typeof(NavigationsMiscellaneousRelationalTestBase<>),
        typeof(NavigationsMiscellaneousTestBase<>),
        typeof(NavigationsPrimitiveCollectionRelationalTestBase<>),
        typeof(NavigationsPrimitiveCollectionTestBase<>),
        typeof(NavigationsProjectionRelationalTestBase<>),
        typeof(NavigationsProjectionTestBase<>),
        typeof(NavigationsSetOperationsRelationalTestBase<>),
        typeof(NavigationsSetOperationsTestBase<>),
        typeof(NavigationsStructuralEqualityRelationalTestBase<>),
        typeof(NavigationsStructuralEqualityTestBase<>),

        // Query/Associations/OwnedJson
        typeof(OwnedJsonBulkUpdateRelationalTestBase<>),
        typeof(OwnedJsonCollectionRelationalTestBase<>),
        typeof(OwnedJsonMiscellaneousRelationalTestBase<>),
        typeof(OwnedJsonPrimitiveCollectionRelationalTestBase<>),
        typeof(OwnedJsonProjectionRelationalTestBase<>),
        typeof(OwnedJsonStructuralEqualityRelationalTestBase<>),

        // Query/Associations/OwnedNavigations
        typeof(OwnedNavigationsCollectionRelationalTestBase<>),
        typeof(OwnedNavigationsCollectionTestBase<>),
        typeof(OwnedNavigationsMiscellaneousRelationalTestBase<>),
        typeof(OwnedNavigationsMiscellaneousTestBase<>),
        typeof(OwnedNavigationsPrimitiveCollectionRelationalTestBase<>),
        typeof(OwnedNavigationsPrimitiveCollectionTestBase<>),
        typeof(OwnedNavigationsProjectionRelationalTestBase<>),
        typeof(OwnedNavigationsProjectionTestBase<>),
        typeof(OwnedNavigationsSetOperationsRelationalTestBase<>),
        typeof(OwnedNavigationsSetOperationsTestBase<>),
        typeof(OwnedNavigationsStructuralEqualityRelationalTestBase<>),
        typeof(OwnedNavigationsStructuralEqualityTestBase<>),

        // Query/Associations/OwnedTableSplitting
        typeof(OwnedTableSplittingMiscellaneousRelationalTestBase<>),
        typeof(OwnedTableSplittingPrimitiveCollectionRelationalTestBase<>),
        typeof(OwnedTableSplittingProjectionRelationalTestBase<>),
        typeof(OwnedTableSplittingStructuralEqualityRelationalTestBase<>),

        // Query/Translations
        typeof(ByteArrayTranslationsTestBase<>),
        typeof(EnumTranslationsTestBase<>),
        typeof(GuidTranslationsTestBase<>),
        typeof(MathTranslationsTestBase<>),
        typeof(MiscellaneousTranslationsRelationalTestBase<>),
        typeof(MiscellaneousTranslationsTestBase<>),
        typeof(StringTranslationsRelationalTestBase<>),
        typeof(StringTranslationsTestBase<>),

        // Query/Translations/Operators
        typeof(ArithmeticOperatorTranslationsTestBase<>),
        typeof(BitwiseOperatorTranslationsTestBase<>),
        typeof(ComparisonOperatorTranslationsTestBase<>),
        typeof(LogicalOperatorTranslationsTestBase<>),
        typeof(MiscellaneousOperatorTranslationsTestBase<>),

        // Query/Translations/Temporal
        typeof(DateOnlyTranslationsTestBase<>),
        typeof(DateTimeOffsetTranslationsTestBase<>),
        typeof(DateTimeTranslationsTestBase<>),
        typeof(TimeOnlyTranslationsTestBase<>),
        typeof(TimeSpanTranslationsTestBase<>),

        // Scaffolding
        typeof(CompiledModelRelationalTestBase),
        typeof(CompiledModelTestBase),

        // Types
        typeof(RelationalTypeTestBase<,>),
        typeof(TypeTestBase<,>),

        // Update
        typeof(ComplexCollectionJsonUpdateTestBase<>),
        typeof(JsonUpdateTestBase<>),
        typeof(NonSharedModelUpdatesTestBase),
        typeof(StoredProcedureUpdateTestBase),
        typeof(UpdateSqlGeneratorTestBase),
        typeof(UpdatesRelationalTestBase<>),
        typeof(UpdatesTestBase<>)
    ];

    protected override Assembly TargetAssembly { get; } = typeof(SnowflakeComplianceTest).Assembly;
}

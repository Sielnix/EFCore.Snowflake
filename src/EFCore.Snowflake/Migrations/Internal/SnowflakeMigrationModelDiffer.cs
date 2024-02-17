using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Internal;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update.Internal;

namespace EFCore.Snowflake.Migrations.Internal;

public class SnowflakeMigrationModelDiffer : MigrationsModelDiffer
{
    public SnowflakeMigrationModelDiffer(IRelationalTypeMappingSource typeMappingSource, IMigrationsAnnotationProvider migrationsAnnotationProvider, IRowIdentityMapFactory rowIdentityMapFactory, CommandBatchPreparerDependencies commandBatchPreparerDependencies)
        : base(typeMappingSource, migrationsAnnotationProvider, rowIdentityMapFactory, commandBatchPreparerDependencies)
    {
    }

    protected override IEnumerable<MigrationOperation> Diff(IColumn source, IColumn target, DiffContext diffContext)
    {
        Console.WriteLine($"Diffing {source} with {target}and prop mappings:");
        
        //foreach (var targetPropertyMapping in target.PropertyMappings)
        //{
        //    if (target.Name == "timeOnlyCol")
        //    {
        //        foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(targetPropertyMapping.Property))
        //        {
        //            string name = descriptor.Name;
        //            object value = descriptor.GetValue(targetPropertyMapping.Property);
        //            Console.WriteLine("{0}={1}", name, value);
        //        }
        //    }
        //    //Console.WriteLine($"Core annotation: {targetPropertyMapping.Property[CoreAnnotationNames.ProviderValueComparer]}");
        //    ////target.ProviderValueComparer
        //    //Console.WriteLine($"comparer: {targetPropertyMapping.Property.GetProviderValueComparer() ?? "NULL" as object}");
        //    //Console.WriteLine($"GetEffectiveProviderClrType: {targetPropertyMapping.Property ?? "NULL" as object}");
        //}
        return base.Diff(source, target, diffContext);
    }
}

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.Snowflake.Infrastructure.Internal;
internal class SnowflakeModelValidator : RelationalModelValidator
{
    public SnowflakeModelValidator(ModelValidatorDependencies dependencies, RelationalModelValidatorDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override void Validate(IModel model, IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        base.Validate(model, logger);

        ValidateSchemasSet(model);
    }

    private void ValidateSchemasSet(IModel model)
    {
        //IAnnotation? annotation = model.FindAnnotation(RelationalAnnotationNames.DefaultSchema);
        //if (annotation.HasStringValueSet())
        //{
        //    return;
        //}

        //// if default schema is not set then verify that all tables and views and procedures has schema set
        //IEnumerable<IEntityType> entities = model.GetEntityTypes();
        //foreach (var entity in entities)
        //{
        //    IAnnotation? entityAnnotation = entity.FindAnnotation(RelationalAnnotationNames.Schema);
        //    if (!entityAnnotation.HasStringValueSet())
        //    {
        //        throw new SnowflakeMissingSchemaException(entity.Name, entity.ClrType?.FullName);
        //    }
        //}
    }
}

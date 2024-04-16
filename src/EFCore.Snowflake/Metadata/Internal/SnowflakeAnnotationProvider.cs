using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.Snowflake.Metadata.Internal;
internal class SnowflakeAnnotationProvider : RelationalAnnotationProvider
{
    public SnowflakeAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
        : base(dependencies)
    {
    }

    public override IEnumerable<IAnnotation> For(IColumn column, bool designTime)
    {
        if (!designTime)
        {
            yield break;
        }

        StoreObjectIdentifier table = StoreObjectIdentifier.Table(column.Table.Name, column.Table.Schema);
        IProperty? identityProperty = column.PropertyMappings
            .Select(m => m.Property)
            .FirstOrDefault(p => p.GetValueGenerationStrategy(table) == SnowflakeValueGenerationStrategy.AutoIncrement);

        if (identityProperty != null)
        {
            long? seed = identityProperty.GetIdentitySeed(table);
            int? increment = identityProperty.GetIdentityIncrement(table);

            yield return new Annotation(
                SnowflakeAnnotationNames.ValueGenerationStrategy,
                SnowflakeValueGenerationStrategy.AutoIncrement);

            yield return new Annotation(
                SnowflakeAnnotationNames.Identity,
                FormattableString.Invariant($"START {seed ?? 1} INCREMENT {increment ?? 1}"));
        }
    }
}

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.TestModels.SemiStructuredTypesModel;

public class SemiStructuredTypesDbContext : PoolableDbContext
{
    public SemiStructuredTypesDbContext()
    {
    }

    public SemiStructuredTypesDbContext(DbContextOptions<SemiStructuredTypesDbContext> options)
        : base(options)
    {
    }

    public virtual required DbSet<VariantTable> Variants { get; set; }
    public virtual required DbSet<ArrayTable> Arrays { get; set; }
    public virtual required DbSet<ObjectTable> Objects { get; set; }

    public static void Seed(SemiStructuredTypesDbContext context)
    {
        context.Variants.AddRange(SemiStructuredTypesData.GenerateVariants());
        context.Arrays.AddRange(SemiStructuredTypesData.GenerateArrays());
        context.Objects.AddRange(SemiStructuredTypesData.GenerateObjects());

        context.SaveChanges();
    }
}

using System.Text.Json;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.TestModels.SemiStructuredTypesModel;

public class SemiStructuredTypesData : ISetSource
{
    public static readonly SemiStructuredTypesData Instance = new();
    
    public IReadOnlyList<VariantTable> Variants { get; }
    public IReadOnlyList<ArrayTable> Arrays { get; }
    public IReadOnlyList<ObjectTable> Objects { get; }

    private SemiStructuredTypesData()
    {
        Variants = GenerateVariants().ToList();
        Arrays = GenerateArrays().ToList();
        Objects = GenerateObjects().ToList();
    }

    public IQueryable<TEntity> Set<TEntity>() where TEntity : class
    {
        if (typeof(TEntity) == typeof(VariantTable))
        {
            return (IQueryable<TEntity>)Variants.AsQueryable();
        }

        if (typeof(TEntity) == typeof(ArrayTable))
        {
            return (IQueryable<TEntity>)Arrays.AsQueryable();
        }

        if (typeof(TEntity) == typeof(ObjectTable))
        {
            return (IQueryable<TEntity>)Objects.AsQueryable();
        }

        throw new InvalidOperationException("Invalid entity type: " + typeof(TEntity));
    }

    public static IEnumerable<VariantTable> GenerateVariants()
    {
        long id = 1;
        yield return new VariantTable
        {
            Id = id++
        };

        yield return new VariantTable
        {
            Id = id++,
            VariantColumn = "some_text"
        };

        yield return new VariantTable()
        {
            Id = id++,
            VariantColumn = 5.ToString()
        };

        yield return new VariantTable()
        {
            Id = id++,
            VariantColumn = JsonSerializer.Serialize(new DummyObject1(7, "hello there"))
        };
    }

    public static IEnumerable<ArrayTable> GenerateArrays()
    {
        long id = 1;
        yield return new ArrayTable
        {
            Id = id++,
        };
        yield return new ArrayTable()
        {
            Id = id++,
            ArrayColumn = JsonSerializer.Serialize(new object?[]
                { "text", 42, 56.5m, null, new DummyObject1(8, "hello") })
        };
    }

    public static IEnumerable<ObjectTable> GenerateObjects()
    {
        long id = 1;
        yield return new ObjectTable
        {
            Id = id++,
        };
        yield return new ObjectTable
        {
            Id = id++,
            ObjectColumn = JsonSerializer.Serialize(new DummyObject2("hello", null, new DummyObject1[]
            {
                new DummyObject1(1, "some text"),
                new DummyObject1(2, "Welcome to the jungle")
            }))
        };
    }
}

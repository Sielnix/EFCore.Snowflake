using System.Text.Json.Nodes;
using EFCore.Snowflake.FunctionalTests.TestModels.SemiStructuredTypesModel;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;

public class SemiStructuredTypesTest : QueryTestBase<SemiStructuredTypesTest.SemiStructuredTypesFixture>
{
    public SemiStructuredTypesTest(SemiStructuredTypesFixture fixture)
    : base(fixture)
    {
    }

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public Task Objects(bool async)
    {
        return AssertQuery(
            async,
            s => s.Set<ObjectTable>()
        );
    }

    [ConditionalTheory]
    [MemberData(nameof(IsAsyncData))]
    public Task Arrays(bool async)
    {
        return AssertQuery(
            async,
            s => s.Set<ArrayTable>()
        );
    }

    [ConditionalTheory(Skip = "Bugs in .net connector")]
    [MemberData(nameof(IsAsyncData))]
    public Task Variants(bool async)
    {
        return AssertQuery(
            async,
            s => s.Set<VariantTable>()
        );
    }

    public class SemiStructuredTypesFixture : SharedStoreFixtureBase<SemiStructuredTypesDbContext>, IQueryFixtureBase
    {
        protected override string StoreName => "SemiStructuredTypesTest";
        protected override ITestStoreFactory TestStoreFactory => SnowflakeTestStoreFactory.Instance;

        protected override void Seed(SemiStructuredTypesDbContext context)
        {
            SemiStructuredTypesDbContext.Seed(context);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            modelBuilder.Entity<VariantTable>(
                b =>
                {
                    b.Property(p => p.VariantColumn).HasColumnType("VARIANT");

                });

            modelBuilder.Entity<ObjectTable>(
                b =>
                {
                    b.Property(p => p.ObjectColumn).HasColumnType("OBJECT");

                });

            modelBuilder.Entity<ArrayTable>(
                b =>
                {
                    b.Property(p => p.ArrayColumn).HasColumnType("ARRAY");
                });
        }

        public Func<DbContext> GetContextCreator()
        {
            return () => CreateContext();
        }

        public ISetSource GetExpectedData()
        {
            return SemiStructuredTypesData.Instance;
        }

        public IReadOnlyDictionary<Type, object> EntitySorters { get; } = new Dictionary<Type, Func<object, object>>()
        {
            { typeof(VariantTable), e => ((VariantTable)e).Id },
            { typeof(ArrayTable), e => ((ArrayTable)e).Id },
            { typeof(ObjectTable), e => ((ObjectTable)e).Id },
        }.ToDictionary(e => e.Key, e => (object)e.Value);

        public IReadOnlyDictionary<Type, object> EntityAsserters { get; } = new Dictionary<Type, Action<object, object>>
        {
            {
                typeof(VariantTable), (e, a) =>
                {
                    Assert.Equal(e == null, a == null);
                    if (a != null)
                    {
                        var ee = (VariantTable)e!;
                        var aa = (VariantTable)a;

                        Assert.Equal(ee.Id, aa.Id);
                        Assert.Equal(ee.VariantColumn, aa.VariantColumn);
                    }
                } 
            },
            {
                typeof(ArrayTable), (e, a) =>
                {
                    Assert.Equal(e == null, a == null);
                    if (a != null)
                    {
                        var ee = (ArrayTable)e!;
                        var aa = (ArrayTable)a;

                        Assert.Equal(ee.Id, aa.Id);

                        Assert.Equal(ee.ArrayColumn == null, aa.ArrayColumn == null);
                        if (ee.ArrayColumn != null)
                        {
                            Assert.True(JsonNode.DeepEquals(JsonNode.Parse(ee.ArrayColumn), JsonNode.Parse(aa.ArrayColumn!)));
                        }
                    }
                }
            },
            {
                typeof(ObjectTable), (e, a) =>
                {
                    Assert.Equal(e == null, a == null);
                    if (a != null)
                    {
                        var ee = (ObjectTable)e!;
                        var aa = (ObjectTable)a;

                        Assert.Equal(ee.Id, aa.Id);
                        Assert.Equal(ee.ObjectColumn == null, aa.ObjectColumn == null);
                        if (ee.ObjectColumn != null)
                        {
                            Assert.True(JsonNode.DeepEquals(JsonNode.Parse(ee.ObjectColumn), JsonNode.Parse(aa.ObjectColumn!)));
                        }
                    }
                }
            },
        }.ToDictionary(e => e.Key, e => (object)e.Value);
    }
}

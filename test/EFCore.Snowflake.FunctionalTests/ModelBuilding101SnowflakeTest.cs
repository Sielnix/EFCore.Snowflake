using Microsoft.EntityFrameworkCore;
using System.Runtime.CompilerServices;

namespace EFCore.Snowflake.FunctionalTests;
public class ModelBuilding101SnowflakeTest : ModelBuilding101RelationalTestBase
{
    protected virtual void Model101TestSnowflake([CallerMemberName] string testMame = "")
    {
        var models = new List<ModelMetadata>();
        // ONLY CHANGE: namespace
        var testTypeName = "EFCore.Snowflake.FunctionalTests.ModelBuilding101SnowflakeTest+" + testMame.Substring(0, testMame.Length - 4);

        foreach (Context101? context in Type.GetType(testTypeName, throwOnError: true)!.GetNestedTypes()
                     .Where(t => t.IsAssignableTo(typeof(DbContext)))
                     .Select(Activator.CreateInstance))
        {
            context!.ConfigureAction = b => ConfigureContext(b);
            models.Add(GetModelMetadata(context));
            context.Dispose();
        }

        Assert.True(models.Count >= 2);

        for (var i = 1; i < models.Count; i++)
        {
            Assert.Equal(models[0], models[i]);
        }
    }

    public override void BasicManyToManyTest()
        => Model101TestSnowflake();

    protected new class BasicManyToMany
    {
        public class Post
        {
            public int Id { get; set; }
            public List<Tag> Tags { get; } = new();
        }

        public class Tag
        {
            public int Id { get; set; }
            public List<Post> Posts { get; } = new();
        }

        public class Context0 : Context101
        {
            public DbSet<Post> Posts
                => Set<Post>();

            public DbSet<Tag> Tags
                => Set<Tag>();
        }

        public class Context1 : Context0
        {
            protected override void OnModelCreating(ModelBuilder modelBuilder)
                => modelBuilder.Entity<Post>()
                    .HasMany(e => e.Tags)
                    .WithMany(e => e.Posts);
        }

        public class Context2 : Context0
        {
            protected override void OnModelCreating(ModelBuilder modelBuilder)
                => modelBuilder.Entity<Post>()
                    .HasMany(e => e.Tags)
                    .WithMany(e => e.Posts)
                    .UsingEntity(
                        "PostTag",
                        l => l.HasOne(typeof(Tag)).WithMany().HasForeignKey("TagsId").HasPrincipalKey(nameof(Tag.Id))
                            // CHANGE: DeleteBehavior - use hybrid
                            .OnDelete(DeleteBehavior.NoAction),
                        r => r.HasOne(typeof(Post)).WithMany().HasForeignKey("PostsId").HasPrincipalKey(nameof(Post.Id))
                            // CHANGE: DeleteBehavior - use hybrid
                            .OnDelete(DeleteBehavior.NoAction),
                        j => j.HasKey("PostsId", "TagsId"));
        }
    }

    [ConditionalFact]
    public override void UnidirectionalManyToManyTest()
        => Model101TestSnowflake();

    protected new class UnidirectionalManyToMany
    {
        public class Post
        {
            public int Id { get; set; }
            public List<Tag> Tags { get; } = new();
        }

        public class Tag
        {
            public int Id { get; set; }
        }

        public class Context0 : Context101
        {
            public DbSet<Post> Posts
                => Set<Post>();

            public DbSet<Tag> Tags
                => Set<Tag>();

            protected override void OnModelCreating(ModelBuilder modelBuilder)
                => modelBuilder.Entity<Post>()
                    .HasMany(e => e.Tags)
                    .WithMany();
        }

        public class Context1 : Context0
        {
            protected override void OnModelCreating(ModelBuilder modelBuilder)
                => modelBuilder.Entity<Post>()
                    .HasMany(e => e.Tags)
                    .WithMany()
                    .UsingEntity(
                        "PostTag",
                        l => l.HasOne(typeof(Tag)).WithMany().HasForeignKey("TagsId").HasPrincipalKey(nameof(Tag.Id))
                            // CHANGE: DeleteBehavior - use hybrid
                            .OnDelete(DeleteBehavior.NoAction),
                        r => r.HasOne(typeof(Post)).WithMany().HasForeignKey("PostId").HasPrincipalKey(nameof(Post.Id))
                            // CHANGE: DeleteBehavior - use hybrid
                            .OnDelete(DeleteBehavior.NoAction),
                        j => j.HasKey("PostId", "TagsId"));
        }
    }

    [ConditionalFact(Skip = "https://github.com/dotnet/efcore/issues/33531")]
    public override void OneToManyRequiredWithAlternateKeyNrtTest()
        => base.OneToManyRequiredWithAlternateKeyNrtTest();

    [ConditionalFact(Skip = "https://github.com/dotnet/efcore/issues/33531")]
    public override void OneToManyRequiredWithAlternateKeyTest()
        => base.OneToManyRequiredWithAlternateKeyTest();

    [ConditionalFact(Skip = "https://github.com/dotnet/efcore/issues/33531")]
    public override void OneToManyRequiredWithShadowFkWithAlternateKeyTest()
        => base.OneToManyRequiredWithShadowFkWithAlternateKeyTest();

    protected override DbContextOptionsBuilder ConfigureContext(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseSnowflake();
}


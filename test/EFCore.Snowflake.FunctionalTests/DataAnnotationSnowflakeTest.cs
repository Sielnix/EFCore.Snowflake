using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;

namespace EFCore.Snowflake.FunctionalTests;
public class DataAnnotationSnowflakeTest(DataAnnotationSnowflakeTest.DataAnnotationSnowflakeFixture fixture)
    : DataAnnotationRelationalTestBase<DataAnnotationSnowflakeTest.DataAnnotationSnowflakeFixture>(fixture)
{
    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
        => facade.UseTransaction(transaction.GetDbTransaction());

    protected override TestHelpers TestHelpers
        => SnowflakeTestHelpers.Instance;

    public override void ForeignKeyAttribute_configures_two_self_referencing_relationships()
    {
        var modelBuilder = CreateModelBuilder();

        modelBuilder.Entity<Comment>();

        var model = modelBuilder.FinalizeModel();

        var entityType = model.FindEntityType(typeof(Comment));
        var fk1 = entityType!.GetForeignKeys().Single(fk => fk.Properties.Single().Name == nameof(Comment.ParentCommentID));
        Assert.Equal(nameof(Comment.ParentComment), fk1!.DependentToPrincipal!.Name);
        Assert.Null(fk1.PrincipalToDependent);
        // TODO: reimplement this test second time on hybrid tables
        //var index1 = entityType.FindIndex(fk1.Properties);
        //Assert.False(index1.IsUnique);

        var fk2 = entityType.GetForeignKeys().Single(fk => fk.Properties.Single().Name == nameof(Comment.ReplyCommentID));
        Assert.Equal(nameof(Comment.ReplyComment), fk2!.DependentToPrincipal!.Name);
        Assert.Null(fk2.PrincipalToDependent);
        //var index2 = entityType.FindIndex(fk2.Properties);
        //Assert.False(index2.IsUnique);

        Assert.Equal(2, entityType.GetForeignKeys().Count());
        //Assert.Equal(2, entityType.GetIndexes().Count());
    }

    public override void TimestampAttribute_throws_if_value_in_database_changed()
        => ExecuteWithStrategyInTransaction(
            context =>
            {
                var clientRow = context.Set<Two>().First(r => r.Id == 1);
                clientRow.Data = "ChangedData";

                using var innerContext = CreateContext();
                UseTransaction(innerContext.Database, context.Database.CurrentTransaction!);
                var storeRow = innerContext.Set<Two>().First(r => r.Id == 1);
                storeRow.Data = "ModifiedData";

                innerContext.SaveChanges();

                // ONLY CHANGE - removed concurrency exception. It should be handled properly by Hybrid tables
                //Assert.Throws<DbUpdateConcurrencyException>(() => context.SaveChanges());
            });

    private class Comment
    {
        [Key]
        public long CommentID { get; set; }

        public long? ReplyCommentID { get; set; }

        public long? ParentCommentID { get; set; }

        [ForeignKey("ParentCommentID")]
        public virtual Comment? ParentComment { get; set; }

        [ForeignKey("ReplyCommentID")]
        public virtual Comment? ReplyComment { get; set; }
    }

    public class DataAnnotationSnowflakeFixture : DataAnnotationRelationalFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;
    }
}

using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Snowflake.Query.Internal;

public class SnowflakeSqlTranslatingExpressionVisitorFactory : IRelationalSqlTranslatingExpressionVisitorFactory
{
    private readonly RelationalSqlTranslatingExpressionVisitorDependencies _dependencies;

    public SnowflakeSqlTranslatingExpressionVisitorFactory(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public RelationalSqlTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        => new SnowflakeSqlTranslatingExpressionVisitor(
            _dependencies,
            queryCompilationContext,
            queryableMethodTranslatingExpressionVisitor);
}

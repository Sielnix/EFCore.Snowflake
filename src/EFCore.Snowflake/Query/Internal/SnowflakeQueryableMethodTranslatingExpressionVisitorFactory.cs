using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Snowflake.Query.Internal;

public class SnowflakeQueryableMethodTranslatingExpressionVisitorFactory : IQueryableMethodTranslatingExpressionVisitorFactory
{
    public SnowflakeQueryableMethodTranslatingExpressionVisitorFactory(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies)
    {
        Dependencies = dependencies;
        RelationalDependencies = relationalDependencies;
    }

    protected virtual QueryableMethodTranslatingExpressionVisitorDependencies Dependencies { get; }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected virtual RelationalQueryableMethodTranslatingExpressionVisitorDependencies RelationalDependencies { get; }

    public QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
    {
        return new SnowflakeQueryableMethodTranslatingExpressionVisitor(
            Dependencies,
            RelationalDependencies,
            queryCompilationContext);
    }
}

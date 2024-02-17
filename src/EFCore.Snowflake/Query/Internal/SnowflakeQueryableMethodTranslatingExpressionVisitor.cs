using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Snowflake.Query.Internal;

public class SnowflakeQueryableMethodTranslatingExpressionVisitor : RelationalQueryableMethodTranslatingExpressionVisitor
{
    public SnowflakeQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        QueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
    }

    protected SnowflakeQueryableMethodTranslatingExpressionVisitor(RelationalQueryableMethodTranslatingExpressionVisitor parentVisitor) : base(parentVisitor)
    {
    }

    public override Expression Translate(Expression expression)
    {
        return base.Translate(expression);
        //while (true)
        //{
        //    try
        //    {
        //        return base.Translate(expression);
        //    }
        //    catch (InvalidOperationException e)
        //    {
        //        Console.WriteLine(e);
        //    }
        //}
    }
}

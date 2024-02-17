using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Query.Internal;

public class SnowflakeQueryCompiler : QueryCompiler
{
    public SnowflakeQueryCompiler(
        IQueryContextFactory queryContextFactory,
        ICompiledQueryCache compiledQueryCache,
        ICompiledQueryCacheKeyGenerator compiledQueryCacheKeyGenerator,
        IDatabase database,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger,
        ICurrentDbContext currentContext,
        IEvaluatableExpressionFilter evaluatableExpressionFilter,
        IModel model)
        : base(queryContextFactory, compiledQueryCache, compiledQueryCacheKeyGenerator, database, logger, currentContext, evaluatableExpressionFilter, model)
    {
    }

    //public override Func<QueryContext, TResult> CompileQueryCore<TResult>(IDatabase database, Expression query, IModel model, bool async)
    //{
    //    try
    //    {
    //        return base.CompileQueryCore<TResult>(database, query, model, async);
    //    }
    //    catch (OuterApplyException)
    //    {
    //        IAsyncQueryProvider provider = new EntityQueryProvider(this);
    //        //IQueryable<TResult> wrap = new EntityQueryable<TResult>(provider, query);

    //        var wrapQuery = provider.CreateQuery<TResult>(
    //            Expression.Call(AsSplitQueryMethodInfo.MakeGenericMethod(typeof(TResult)), query));

    //        return base.CompileQueryCore<TResult>(database, wrapQuery.Expression, model, async);
    //        //Console.WriteLine(e);
    //        //throw;
    //    }
        
    //}

    internal static readonly MethodInfo AsSplitQueryMethodInfo
        = typeof(RelationalQueryableExtensions).GetTypeInfo().GetDeclaredMethod(nameof(RelationalQueryableExtensions.AsSplitQuery))!;
}

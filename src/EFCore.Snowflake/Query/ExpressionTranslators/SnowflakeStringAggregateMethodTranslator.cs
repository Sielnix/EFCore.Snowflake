using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Snowflake.Query.ExpressionTranslators;

public class SnowflakeStringAggregateMethodTranslator : IAggregateMethodCallTranslator
{
    private static readonly MethodInfo StringConcatMethod
        = typeof(string).GetRuntimeMethod(nameof(string.Concat), new[] { typeof(IEnumerable<string>) })!;

    private static readonly MethodInfo StringJoinMethod
        = typeof(string).GetRuntimeMethod(nameof(string.Join), new[] { typeof(string), typeof(IEnumerable<string>) })!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public SnowflakeStringAggregateMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        MethodInfo method,
        EnumerableExpression source,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (source.Selector is not SqlExpression sqlExpression
            || (method != StringJoinMethod && method != StringConcatMethod))
        {
            return null;
        }

        // LISTAGG filters out nulls, but string.Join treats them as empty strings; coalesce unless we know we're aggregating over
        // a non-nullable column.
        if (sqlExpression is not ColumnExpression { IsNullable: false })
        {
            sqlExpression = _sqlExpressionFactory.Coalesce(
                sqlExpression,
                _sqlExpressionFactory.Constant(string.Empty, typeof(string)));
        }

        return null;
    }
}

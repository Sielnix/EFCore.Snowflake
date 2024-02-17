using System.Reflection;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Snowflake.Query.ExpressionTranslators;
internal class SnowflakeMathTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<MethodInfo, string> SupportedMethodTranslations = new()
    {
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(decimal) })!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(double) })!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(float) })!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(int) })!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(long) })!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(short) })!, "ABS" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Abs), new[] { typeof(float) })!, "ABS" },
    };

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public SnowflakeMathTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (SupportedMethodTranslations.TryGetValue(method, out var sqlFunctionName))
        {
            var typeMapping = arguments.Count == 1
                ? ExpressionExtensions.InferTypeMapping(arguments[0])
                : ExpressionExtensions.InferTypeMapping(arguments[0], arguments[1]);

            var newArguments = new SqlExpression[arguments.Count];
            newArguments[0] = _sqlExpressionFactory.ApplyTypeMapping(arguments[0], typeMapping);

            if (arguments.Count == 2)
            {
                newArguments[1] = _sqlExpressionFactory.ApplyTypeMapping(arguments[1], typeMapping);
            }

            // Note: GREATER/LEAST only return NULL if *all* arguments are null, but we currently can't
            // convey this.
            return _sqlExpressionFactory.Function(
                sqlFunctionName,
                newArguments,
                nullable: true,
                argumentsPropagateNullability: Statics.TrueArrays[newArguments.Length],
                method.ReturnType,
                typeMapping);
        }

        return null;
    }
}

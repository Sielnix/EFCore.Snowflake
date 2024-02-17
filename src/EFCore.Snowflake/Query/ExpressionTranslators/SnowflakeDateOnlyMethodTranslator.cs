using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Snowflake.Query.ExpressionTranslators;

public class SnowflakeDateOnlyMethodTranslator : IMethodCallTranslator
{
    private readonly Dictionary<MethodInfo, string> _methodInfoDatePartMapping = new()
    {
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddYears), [typeof(int)])!, "year" },
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddMonths), [typeof(int)])!, "month" },
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddDays), [typeof(int)])!, "day" }
    };

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public SnowflakeDateOnlyMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(SqlExpression? instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (_methodInfoDatePartMapping.TryGetValue(method, out var datePart)
            && instance != null)
        {
            instance = _sqlExpressionFactory.ApplyDefaultTypeMapping(instance);

            return _sqlExpressionFactory.Function(
                "DATEADD",
                new[] { _sqlExpressionFactory.Fragment(datePart), _sqlExpressionFactory.Convert(arguments[0], typeof(int)), instance },
                nullable: true,
                argumentsPropagateNullability: new[] { false, true, true },
                instance.Type,
                instance.TypeMapping);
        }

        if (method.DeclaringType == typeof(DateOnly)
            && method.Name == nameof(DateOnly.FromDateTime)
            && arguments.Count == 1)
        {
            return _sqlExpressionFactory.Convert(arguments[0], typeof(DateOnly));
        }

        return null;
    }
}

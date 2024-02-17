using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Snowflake.Query.ExpressionTranslators;

public class SnowflakeDateTimeMethodTranslator : IMethodCallTranslator
{
    private readonly Dictionary<MethodInfo, string> _methodInfoDatePartMapping = new()
    {
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddYears), new[] { typeof(int) })!, "year" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMonths), new[] { typeof(int) })!, "month" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddDays), new[] { typeof(double) })!, "day" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddHours), new[] { typeof(double) })!, "hour" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMinutes), new[] { typeof(double) })!, "minute" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddSeconds), new[] { typeof(double) })!, "second" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMilliseconds), new[] { typeof(double) })!, "millisecond" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddYears), new[] { typeof(int) })!, "year" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddMonths), new[] { typeof(int) })!, "month" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddDays), new[] { typeof(double) })!, "day" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddHours), new[] { typeof(double) })!, "hour" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddMinutes), new[] { typeof(double) })!, "minute" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddSeconds), new[] { typeof(double) })!, "second" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddMilliseconds), new[] { typeof(double) })!, "millisecond" }
    };
    
    private static readonly Dictionary<MethodInfo, string> _methodInfoDateDiffMapping = new()
    {
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.ToUnixTimeSeconds), Type.EmptyTypes)!, "second" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.ToUnixTimeMilliseconds), Type.EmptyTypes)!, "millisecond" }
    };


    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public SnowflakeDateTimeMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (_methodInfoDatePartMapping.TryGetValue(method, out var datePart)
            && instance != null)
        {
            if (arguments[0] is SqlConstantExpression { Value: double and (<= int.MinValue or >= int.MaxValue) })
            {
                return null;
            }

            return _sqlExpressionFactory.Function(
                "DATEADD",
                new[] { _sqlExpressionFactory.Fragment(datePart), _sqlExpressionFactory.Convert(arguments[0], typeof(int)), instance },
                nullable: true,
                argumentsPropagateNullability: new[] { false, true, true },
                instance.Type,
                instance.TypeMapping);
        }

        if (_methodInfoDateDiffMapping.TryGetValue(method, out var timePart))
        {
            return _sqlExpressionFactory.Function(
                "TIMESTAMPDIFF",
                new[]
                {
                    _sqlExpressionFactory.Fragment(timePart),
                    _sqlExpressionFactory.Constant(DateTimeOffset.UnixEpoch, instance!.TypeMapping),
                    instance
                },
                nullable: true,
                argumentsPropagateNullability: new[] { false, true, true },
                typeof(long));
        }

        return null;
    }
}

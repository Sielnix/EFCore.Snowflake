using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Snowflake.Query.ExpressionTranslators;

public class SnowflakeTimeOnlyMethodTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo AddHoursMethod = typeof(TimeOnly).GetRuntimeMethod(
        nameof(TimeOnly.AddHours), new[] { typeof(double) })!;

    private static readonly MethodInfo AddMinutesMethod = typeof(TimeOnly).GetRuntimeMethod(
        nameof(TimeOnly.AddMinutes), new[] { typeof(double) })!;

    private static readonly MethodInfo IsBetweenMethod = typeof(TimeOnly).GetRuntimeMethod(
        nameof(TimeOnly.IsBetween), new[] { typeof(TimeOnly), typeof(TimeOnly) })!;

    private static readonly MethodInfo Add = typeof(TimeOnly).GetRuntimeMethod(
        nameof(TimeOnly.Add), new[] { typeof(TimeSpan) })!;

    private static readonly MethodInfo FromDateTime = typeof(TimeOnly).GetRuntimeMethod(
        nameof(TimeOnly.FromDateTime), [typeof(DateTime)])!;

    private static readonly MethodInfo FromTimeSpan = typeof(TimeOnly).GetRuntimeMethod(
        nameof(TimeOnly.FromTimeSpan), [typeof(TimeSpan)])!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public SnowflakeTimeOnlyMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }
    
    public SqlExpression? Translate(SqlExpression? instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(TimeOnly))
        {
            return null;
        }

        if ((method == FromDateTime || method == FromTimeSpan)
            && instance is null
            && arguments.Count == 1)
        {
            return _sqlExpressionFactory.Convert(arguments[0], typeof(TimeOnly));
        }

        if (instance is null)
        {
            return null;
        }

        if (method == Add
            && arguments[0] is SqlConstantExpression { Value: TimeSpan timeSpan })
        {
            if (timeSpan == TimeSpan.Zero)
            {
                return instance;
            }
            
            instance = _sqlExpressionFactory.ApplyDefaultTypeMapping(instance);
            if (timeSpan.Hours > 0)
            {
                instance = AddHours(instance, _sqlExpressionFactory.Constant(timeSpan.Hours));
            }

            if (timeSpan.Minutes > 0)
            {
                instance = AddMinutes(instance, _sqlExpressionFactory.Constant(timeSpan.Minutes));
            }

            if (timeSpan.Seconds > 0)
            {
                instance = AddSeconds(instance, _sqlExpressionFactory.Constant(timeSpan.Seconds));
            }

            long nanoSeconds = timeSpan.Milliseconds * 100_000L + timeSpan.Microseconds * 100L + timeSpan.Nanoseconds;
            if (nanoSeconds > 0)
            {
                instance = AddNanoseconds(instance, _sqlExpressionFactory.Constant(nanoSeconds));
            }

            return instance;
        }

        if (method == AddHoursMethod || method == AddMinutesMethod)
        {
            instance = _sqlExpressionFactory.ApplyDefaultTypeMapping(instance);

            return method == AddHoursMethod
                ? AddHours(instance, arguments[0])
                : AddMinutes(instance, arguments[0]);
        }

        if (method == IsBetweenMethod
            && instance is ColumnExpression or SqlConstantExpression or SqlParameterExpression)
        {
            var typeMapping = ExpressionExtensions.InferTypeMapping(instance, arguments[0], arguments[1]);
            instance = _sqlExpressionFactory.ApplyTypeMapping(instance, typeMapping);

            return _sqlExpressionFactory.AndAlso(
                _sqlExpressionFactory.GreaterThanOrEqual(
                    instance,
                    _sqlExpressionFactory.ApplyTypeMapping(arguments[0], typeMapping)),
                _sqlExpressionFactory.LessThan(
                    instance,
                    _sqlExpressionFactory.ApplyTypeMapping(arguments[1], typeMapping)));
        }

        return null;
        
    }

    private SqlExpression AddHours(SqlExpression instance, SqlExpression argument)
    {
        return CreateDateAddFunction(instance, argument, "hour");
    }

    private SqlExpression AddMinutes(SqlExpression instance, SqlExpression argument)
    {
        return CreateDateAddFunction(instance, argument, "minute");
    }

    private SqlExpression AddSeconds(SqlExpression instance, SqlExpression argument)
    {
        return CreateDateAddFunction(instance, argument, "second");
    }

    private SqlExpression AddNanoseconds(SqlExpression instance, SqlExpression argument)
    {
        return CreateDateAddFunction(instance, argument, "nanoseconds");
    }

    private SqlExpression CreateDateAddFunction(SqlExpression instance, SqlExpression argument, string datePart)
    {
        return _sqlExpressionFactory.Function(
            "DATEADD",
            new[] { _sqlExpressionFactory.Fragment(datePart), _sqlExpressionFactory.Convert(argument, typeof(int)), instance },
            nullable: true,
            argumentsPropagateNullability: new[] { false, true, true },
            instance.Type,
            instance.TypeMapping);
    }
}

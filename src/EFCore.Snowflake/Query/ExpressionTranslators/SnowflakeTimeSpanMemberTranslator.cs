using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Snowflake.Query.ExpressionTranslators;
public class SnowflakeTimeSpanMemberTranslator : IMemberTranslator
{
    private static readonly Dictionary<string, string> DatePartMappings = new()
    {
        { nameof(TimeSpan.Hours), "hour" },
        { nameof(TimeSpan.Minutes), "minute" },
        { nameof(TimeSpan.Seconds), "second" },
        //{ nameof(TimeSpan.Milliseconds), "millisecond" }
    };

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public SnowflakeTimeSpanMemberTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        //if (member.DeclaringType == typeof(TimeSpan) && DatePartMappings.TryGetValue(member.Name, out var value))
        //{
        //    return _sqlExpressionFactory.Function(
        //        "DATE_PART", new[] { _sqlExpressionFactory.Fragment(value), instance! },
        //        nullable: true,
        //        argumentsPropagateNullability: new[] { false, true },
        //        returnType);
        //}

        return null;
    }
}

using System.Reflection;
using EFCore.Snowflake.Storage.Internal.Mapping;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Query.ExpressionTranslators;

public class SnowflakeDateTimeMemberTranslator : IMemberTranslator
{
    private static readonly Dictionary<string, string> DatePartMapping
        = new()
        {
            { nameof(DateTime.Year), "year" },
            { nameof(DateTime.Month), "month" },
            { nameof(DateTime.DayOfYear), "dayofyear" },
            { nameof(DateTime.DayOfWeek), "dayofweek" },
            { nameof(DateTime.Day), "day" },
            { nameof(DateTime.Hour), "hour" },
            { nameof(DateTime.Minute), "minute" },
            { nameof(DateTime.Second), "second" },

            // TODO:
            // nanosecond returns all part (milli + micro + nano)
            // bigger query would have to be written to handle that
            //{ nameof(DateTime.Millisecond), "millisecond" },
            //{ nameof(DateTime.Microsecond), "microsecond" },
            //{ nameof(DateTime.Nanosecond), "nanosecond" },
        };

    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public SnowflakeDateTimeMemberTranslator(IRelationalTypeMappingSource typeMappingSource, ISqlExpressionFactory sqlExpressionFactory)
    {
        _typeMappingSource = typeMappingSource;
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(SqlExpression? instance, MemberInfo member, Type returnType, IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        var declaringType = member.DeclaringType;

        if (declaringType != typeof(DateTime)
            && declaringType != typeof(DateTimeOffset)
            && declaringType != typeof(DateOnly)
            && declaringType != typeof(TimeOnly))
        {
            return null;
        }

        if (declaringType == typeof(DateTimeOffset)
            && instance is not null
            && TranslateDateTimeOffset(instance, member) is { } translated)
        {
            return translated;
        }

        var memberName = member.Name;

        if (DatePartMapping.TryGetValue(memberName, out var datePart))
        {
            SqlExpression toConvert = instance!;
            if (declaringType == typeof(DateTimeOffset))
            {
                toConvert = _sqlExpressionFactory.Function(
                    name: "CONVERT_TIMEZONE",
                    arguments: [ _sqlExpressionFactory.Constant("UTC"), toConvert],
                    nullable: true,
                    argumentsPropagateNullability: new[] { false, true },
                    returnType: typeof(DateTime));
            }

            return _sqlExpressionFactory.Function(
                "DATE_PART",
                [_sqlExpressionFactory.Fragment(datePart), toConvert],
                nullable: true,
                argumentsPropagateNullability: new[] { false, true },
                returnType);
        }

        switch (memberName)
        {
            case nameof(DateTime.UtcNow):
            {
                if (declaringType == typeof(DateTimeOffset))
                {
                    throw new InvalidOperationException(
                        $"{nameof(DateTimeOffset)}.{nameof(DateTimeOffset.UtcNow)} is not supported in Snowflake. You can use {nameof(DateTime)}.{nameof(DateTime.UtcNow)}");
                }

                return _sqlExpressionFactory.Function(
                    "SYSDATE",
                    Enumerable.Empty<SqlExpression>(),
                    nullable: false,
                    argumentsPropagateNullability: Array.Empty<bool>(),
                    returnType);
            }
        }

        return null;
    }

    public virtual SqlExpression? TranslateDateTimeOffset(SqlExpression instance, MemberInfo member)
    {
        if (member.Name == nameof(DateTimeOffset.Date))
        {
            RelationalTypeMapping? castedTypeMapping = _typeMappingSource.FindMapping(SnowflakeStoreTypeNames.GetTimeType(SnowflakeStoreTypeNames.TimestampNtz, SnowflakeStoreTypeNames.DefaultTimePrecision));
            var converted = _sqlExpressionFactory.Convert(instance, typeof(DateTime), castedTypeMapping);

            return _sqlExpressionFactory.Function(
                "DATE_TRUNC",
                new SqlExpression[]
                {
                    _sqlExpressionFactory.Constant("day"),
                    converted
                },
                nullable: true,
                argumentsPropagateNullability: Statics.TrueArrays[2],
                typeof(DateTime),
                typeMapping: castedTypeMapping);
        }

        return null;
    }
}

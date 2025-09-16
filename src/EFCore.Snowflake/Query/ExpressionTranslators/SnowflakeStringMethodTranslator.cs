using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Diagnostics;
using System.Reflection;

namespace EFCore.Snowflake.Query.ExpressionTranslators;

public class SnowflakeStringMethodTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo ToLowerMethodInfo
        = typeof(string).GetRuntimeMethod(nameof(string.ToLower), Type.EmptyTypes)!;

    private static readonly MethodInfo ToUpperMethodInfo
        = typeof(string).GetRuntimeMethod(nameof(string.ToUpper), Type.EmptyTypes)!;

    private static readonly MethodInfo SubstringMethodInfoWithOneArg
        = typeof(string).GetRuntimeMethod(nameof(string.Substring), new[] { typeof(int) })!;

    private static readonly MethodInfo SubstringMethodInfoWithTwoArgs
        = typeof(string).GetRuntimeMethod(nameof(string.Substring), new[] { typeof(int), typeof(int) })!;

    private static readonly MethodInfo TrimStartMethodInfoWithoutArgs
        = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), Type.EmptyTypes)!;

    private static readonly MethodInfo TrimStartMethodInfoWithCharArg
        = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), [typeof(char)])!;

    private static readonly MethodInfo TrimStartMethodInfoWithCharArrayArg
        = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), [typeof(char[])])!;

    private static readonly MethodInfo TrimEndMethodInfoWithoutArgs
        = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), Type.EmptyTypes)!;

    private static readonly MethodInfo TrimEndMethodInfoWithCharArg
        = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), [typeof(char)])!;

    private static readonly MethodInfo TrimEndMethodInfoWithCharArrayArg
        = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), [typeof(char[])])!;

    private static readonly MethodInfo TrimMethodInfoWithoutArgs
        = typeof(string).GetRuntimeMethod(nameof(string.Trim), Type.EmptyTypes)!;

    private static readonly MethodInfo TrimMethodInfoWithCharArg
        = typeof(string).GetRuntimeMethod(nameof(string.Trim), [typeof(char)])!;
    
    private static readonly MethodInfo TrimMethodInfoWithCharArrayArg
        = typeof(string).GetRuntimeMethod(nameof(string.Trim), [typeof(char[])])!;

    private static readonly MethodInfo FirstOrDefaultMethodInfoWithoutArgs
        = typeof(Enumerable).GetRuntimeMethods().Single(
            m => m.Name == nameof(Enumerable.FirstOrDefault)
                 && m.GetParameters().Length == 1).MakeGenericMethod(typeof(char));

    private static readonly MethodInfo LastOrDefaultMethodInfoWithoutArgs
        = typeof(Enumerable).GetRuntimeMethods().Single(
            m => m.Name == nameof(Enumerable.LastOrDefault)
                 && m.GetParameters().Length == 1).MakeGenericMethod(typeof(char));

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public SnowflakeStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(SqlExpression? instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance != null)
        {
            if (ToLowerMethodInfo.Equals(method)
                || ToUpperMethodInfo.Equals(method))
            {
                return _sqlExpressionFactory.Function(
                    ToLowerMethodInfo.Equals(method) ? "LOWER" : "UPPER",
                    new[] { instance },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    method.ReturnType,
                    instance.TypeMapping);
            }


            if (SubstringMethodInfoWithOneArg.Equals(method))
            {
                return _sqlExpressionFactory.Function(
                    "SUBSTRING",
                    new[]
                    {
                        instance,
                        _sqlExpressionFactory.Add(
                            arguments[0],
                            _sqlExpressionFactory.Constant(1))
                    },
                    nullable: true,
                    argumentsPropagateNullability: Statics.TrueArrays[2],
                    method.ReturnType,
                    instance.TypeMapping);
            }

            if (SubstringMethodInfoWithTwoArgs.Equals(method))
            {

                return _sqlExpressionFactory.Function(
                    "SUBSTRING",
                    new[]
                    {
                        instance,
                        _sqlExpressionFactory.Add(
                            arguments[0],
                            _sqlExpressionFactory.Constant(1)),
                        arguments[1]
                    },
                    nullable: true,
                    argumentsPropagateNullability: Statics.TrueArrays[3],
                    method.ReturnType,
                    instance.TypeMapping);
            }

            if (TrimStartMethodInfoWithoutArgs.Equals(method)
                || TrimStartMethodInfoWithCharArg.Equals(method)
                || TrimStartMethodInfoWithCharArrayArg.Equals(method))
            {
                return ProcessTrimMethod(instance, arguments, "LTRIM");
            }

            if (TrimEndMethodInfoWithoutArgs.Equals(method)
                || TrimEndMethodInfoWithCharArg.Equals(method)
                || TrimEndMethodInfoWithCharArrayArg.Equals(method))
            {
                return ProcessTrimMethod(instance, arguments, "RTRIM");
            }

            if (TrimMethodInfoWithoutArgs.Equals(method)
                || TrimMethodInfoWithCharArg.Equals(method)
                || TrimMethodInfoWithCharArrayArg.Equals(method))
            {
                return ProcessTrimMethod(instance, arguments, "TRIM");
            }
        }

        if (FirstOrDefaultMethodInfoWithoutArgs.Equals(method))
        {
            var argument = arguments[0];
            return _sqlExpressionFactory.Function(
                "SUBSTRING",
                new[] { argument, _sqlExpressionFactory.Constant(1), _sqlExpressionFactory.Constant(1) },
                nullable: true,
                argumentsPropagateNullability: new[] { true, true, true },
                method.ReturnType);
        }

        if (LastOrDefaultMethodInfoWithoutArgs.Equals(method))
        {
            var argument = arguments[0];
            return _sqlExpressionFactory.Function(
                "SUBSTRING",
                new[]
                {
                    argument,
                    _sqlExpressionFactory.Function(
                        "LENGTH",
                        new[] { argument },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true },
                        typeof(int)),
                    _sqlExpressionFactory.Constant(1)
                },
                nullable: true,
                argumentsPropagateNullability: new[] { true, true, true },
                method.ReturnType);
        }

        return null;
    }

    private SqlExpression? ProcessTrimMethod(SqlExpression instance, IReadOnlyList<SqlExpression> arguments, string functionName)
    {
        SqlExpression? charactersToTrim = null;
        if (arguments.Count > 0 && arguments[0] is SqlConstantExpression { Value: var charactersToTrimValue })
        {
            charactersToTrim = charactersToTrimValue switch
            {
                char singleChar => _sqlExpressionFactory.Constant(singleChar.ToString(), instance.TypeMapping),
                char[] charArray => _sqlExpressionFactory.Constant(new string(charArray), instance.TypeMapping),
                _ => throw new ArgumentException("Invalid parameter type for string.Trim")
            };
        }

        return _sqlExpressionFactory.Function(
            functionName,
            arguments: charactersToTrim is null ? [instance] : [instance, charactersToTrim],
            nullable: true,
            argumentsPropagateNullability: charactersToTrim is null ? [true] : [true, true],
            instance.Type,
            instance.TypeMapping);
    }
}

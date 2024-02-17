using System.Reflection;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

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
}

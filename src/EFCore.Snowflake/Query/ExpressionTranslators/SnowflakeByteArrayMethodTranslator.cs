using System.Reflection;
using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Snowflake.Query.ExpressionTranslators;

public class SnowflakeByteArrayMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public SnowflakeByteArrayMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }
    
    public SqlExpression? Translate(SqlExpression? instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.IsGenericMethod)
        {
            if (method.GetGenericMethodDefinition().Equals(EnumerableMethods.Contains)
                && arguments[0].Type == typeof(byte[]))
            {
                var source = arguments[0];
                var sourceTypeMapping = source.TypeMapping;

                var value = arguments[1] is SqlConstantExpression constantValue
                    ? (SqlExpression)_sqlExpressionFactory.Constant(new[] { (byte)constantValue.Value! },
                        sourceTypeMapping)
                    : _sqlExpressionFactory.Convert(arguments[1], typeof(byte[]), sourceTypeMapping);

                return _sqlExpressionFactory.GreaterThan(
                    _sqlExpressionFactory.Function(
                        "POSITION",
                        new[] { value, source },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true, true },
                        typeof(int)),
                    _sqlExpressionFactory.Constant(0));
            }

            if (method.GetGenericMethodDefinition().Equals(EnumerableMethods.FirstWithoutPredicate))
            {
                return _sqlExpressionFactory.Convert(
                    _sqlExpressionFactory.Function(
                        "SUBSTRING",
                        new[] { arguments[0], _sqlExpressionFactory.Constant(1), _sqlExpressionFactory.Constant(1) },
                        nullable: true,
                        argumentsPropagateNullability: Statics.TrueArrays[3],
                        returnType: typeof(byte[])),
                    method.ReturnType);
            }
        }

        return null;
    }
}

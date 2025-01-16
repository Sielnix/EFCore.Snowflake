using EFCore.Snowflake.Storage.Internal.Mapping;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Reflection;

namespace EFCore.Snowflake.Query.ExpressionTranslators;

public class SnowflakeObjectToStringTranslator : IMethodCallTranslator
{
    private static readonly HashSet<Type> SupportedTypes =
        [
            typeof(bool),
            typeof(sbyte),
            typeof(byte),
            typeof(short),
            typeof(ushort),
            typeof(int),
            typeof(uint),
            typeof(long),
            typeof(ulong),
            typeof(float),
            typeof(double),
            typeof(decimal),
            typeof(char),
            typeof(DateTime),
            typeof(DateOnly),
            typeof(TimeOnly),
            typeof(DateTimeOffset),
            typeof(TimeSpan),
            typeof(Guid),
            typeof(byte[])
        ];

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public SnowflakeObjectToStringTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance == null || method.Name != nameof(ToString) || arguments.Count != 0)
        {
            return null;
        }

        if (instance.TypeMapping?.ClrType == typeof(string)
            || instance.Type == typeof(string))
        {
            return instance;
        }

        if (!SupportedTypes.Contains(instance.Type))
        {
            return null;
        }

        if (instance.Type == typeof(bool))
        {
            if (instance is ColumnExpression { IsNullable: true })
            {
                return _sqlExpressionFactory.Case(
                    [
                        new CaseWhenClause(
                            _sqlExpressionFactory.Equal(instance, _sqlExpressionFactory.Constant(false)),
                            _sqlExpressionFactory.Constant(false.ToString())),
                        new CaseWhenClause(
                            _sqlExpressionFactory.Equal(instance, _sqlExpressionFactory.Constant(true)),
                            _sqlExpressionFactory.Constant(true.ToString()))
                    ],
                    _sqlExpressionFactory.Constant(string.Empty));
            }

            return _sqlExpressionFactory.Case(
                [
                    new CaseWhenClause(
                        _sqlExpressionFactory.Equal(instance, _sqlExpressionFactory.Constant(false)),
                        _sqlExpressionFactory.Constant(false.ToString()))
                ],
                _sqlExpressionFactory.Constant(true.ToString()));
        }

        SqlExpression result = _sqlExpressionFactory.Function(
            name: "TO_VARCHAR",
            arguments: [instance],
            nullable: true,
            argumentsPropagateNullability: Statics.TrueArrays[1],
            returnType: typeof(string));

        if (instance.Type == typeof(byte)
            &&
            (instance.TypeMapping == null
             || instance.TypeMapping.StoreTypeNameBase != SnowflakeStoreTypeNames.Number))
        {
            // System.Byte can be represented as BINARY(1) or NUMBER(3)
            // if it's binary then Snowflake doesn't have direct conversion to string
            // representing decimal value of byte (TO_VARCHAR returns hexadecimal value)
            // so we convert to string, then to number from hex string, then again to string

            // wrap into to number
            result = _sqlExpressionFactory.Function(
                "TO_NUMBER",
                [result, _sqlExpressionFactory.Constant("XXX")],
                nullable: true,
                argumentsPropagateNullability: Statics.TrueArrays[2],
                returnType: typeof(int));

            // wrap into to varchar
            result = _sqlExpressionFactory.Function(
                "TO_VARCHAR",
                [result],
                nullable: true,
                argumentsPropagateNullability: Statics.TrueArrays[1],
                typeof(string));
        }

        return result;
    }
}

using System.Linq.Expressions;
using System.Reflection;
using EFCore.Snowflake.Storage.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Internal;

namespace EFCore.Snowflake.Query;

internal class SnowflakeQuerySqlGenerator : QuerySqlGenerator
{
    private static readonly Dictionary<ExpressionType, string> binaryFunctions = new()
    {
        { ExpressionType.And, "BITAND" },
        { ExpressionType.Or, "BITOR" }
    };

    private readonly SnowflakeSqlGenerationHelper _sqlGenerationHelper;

    private Dictionary<string, int>? _repeatedParameterCounts;

    public SnowflakeQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
        _sqlGenerationHelper = (SnowflakeSqlGenerationHelper)dependencies.SqlGenerationHelper;
    }

    protected override string GetOperator(SqlBinaryExpression binaryExpression)
        => binaryExpression.OperatorType == ExpressionType.Add
           && binaryExpression.Type == typeof(string)
            ? " || "
            : base.GetOperator(binaryExpression);

    protected override Expression VisitSqlBinary(SqlBinaryExpression sqlBinaryExpression)
    {
        if (binaryFunctions.TryGetValue(sqlBinaryExpression.OperatorType, out string? sqlFunction))
        {
            Sql.Append(sqlFunction).Append("(");
            Visit(sqlBinaryExpression.Left);
            Sql.Append(", ");
            Visit(sqlBinaryExpression.Right);
            Sql.Append(")");

            return sqlBinaryExpression;
        }

        return base.VisitSqlBinary(sqlBinaryExpression);
    }

    protected override Expression VisitOuterApply(OuterApplyExpression outerApplyExpression)
    {
        throw new SnowflakeOuterApplyNotSupportedException();
    }

    protected override Expression VisitCrossApply(CrossApplyExpression crossApplyExpression)
    {
        Sql.Append("INNER JOIN LATERAL ");
        Visit(crossApplyExpression.Table);

        return crossApplyExpression;
    }

    protected override Expression VisitOrdering(OrderingExpression orderingExpression)
    {
        Expression result = base.VisitOrdering(orderingExpression);

        Sql.Append(orderingExpression.IsAscending ? " NULLS FIRST" : " NULLS LAST");

        return result;
    }

    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        if (selectExpression.Offset != null)
        {
            Sql.AppendLine()
                .Append("OFFSET ");

            Visit(selectExpression.Offset);

            Sql.Append(" ROWS");

            Sql.Append(" FETCH NEXT ");

            if (selectExpression.Limit != null)
            {
                Visit(selectExpression.Limit);
            }
            else
            {
                Sql.Append("NULL");
            }

            Sql.Append(" ROWS ONLY");
        }
        else if (selectExpression.Limit != null)
        {
            Sql.AppendLine()
                .Append("FETCH FIRST ");

            Visit(selectExpression.Limit);

            Sql.Append(" ROWS ONLY");
        }
    }

    protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
    {
        var invariantName = sqlParameterExpression.Name;
        var parameterName = sqlParameterExpression.Name;
        var typeMapping = sqlParameterExpression.TypeMapping!;

        // rewritten to access internal property
        var parameter = Sql.Parameters.FirstOrDefault(p => IsSameParameter(p));

        if (parameter is null)
        {
            parameterName = GetUniqueParameterName(parameterName);

            Sql.AddParameter(
                invariantName,
                _sqlGenerationHelper.GenerateParameterName(parameterName),
                sqlParameterExpression.TypeMapping!,
                sqlParameterExpression.IsNullable);
        }
        else
        {
            parameterName = ((TypeMappedRelationalParameter)parameter).Name;
        }

        Sql.Append(_sqlGenerationHelper.GenerateParameterNamePlaceholder(parameterName, sqlParameterExpression.TypeMapping));

        return sqlParameterExpression;

        string GetUniqueParameterName(string currentName)
        {
            _repeatedParameterCounts ??= new Dictionary<string, int>();

            if (!_repeatedParameterCounts.TryGetValue(currentName, out var currentCount))
            {
                _repeatedParameterCounts[currentName] = 0;

                return currentName;
            }

            currentCount++;
            _repeatedParameterCounts[currentName] = currentCount;

            return currentName + "_" + currentCount;
        }

        bool IsSameParameter(IRelationalParameter p)
        {
            if (p.InvariantName != parameterName)
            {
                return false;
            }

            if (p is not TypeMappedRelationalParameter typeMappedParameter)
            {
                return false;
            }

            MethodInfo getMethod = typeof(TypeMappedRelationalParameter)
                .GetProperty("RelationalTypeMapping", BindingFlags.Instance | BindingFlags.NonPublic)?
                .GetMethod ?? throw new InvalidOperationException("Changed underlying contract");

            RelationalTypeMapping? existingTypeMapping = (RelationalTypeMapping?)getMethod.Invoke(typeMappedParameter, Array.Empty<object>());
            if (existingTypeMapping is null)
            {
                return false;
            }

            return string.Equals(existingTypeMapping.StoreType, typeMapping.StoreType, StringComparison.OrdinalIgnoreCase)
                   && (existingTypeMapping.Converter is null && typeMapping.Converter is null
                       || existingTypeMapping.Converter is not null && existingTypeMapping.Converter.Equals(typeMapping.Converter));
        }
    }

    ///// <summary>
    ///// IT IS COPY & PASTE from base method EXCEPT it doesn't check if parameter was already used for its re-usage.
    ///// </summary>
    ///// <param name="sqlParameterExpression"></param>
    ///// <returns></returns>
    //protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
    //{
    //    string invariantName = sqlParameterExpression.Name;
    //    string parameterName = sqlParameterExpression.Name;

    //    parameterName = GetUniqueParameterName(parameterName);
    //    Sql.AddParameter(
    //        invariantName,
    //        _sqlGenerationHelper.GenerateParameterName(parameterName),
    //        sqlParameterExpression.TypeMapping!,
    //        sqlParameterExpression.IsNullable);

    //    Sql.Append(_sqlGenerationHelper.GenerateParameterNamePlaceholder(parameterName));

    //    return sqlParameterExpression;

    //    string GetUniqueParameterName(string currentName)
    //    {
    //        _repeatedParameterCounts ??= new Dictionary<string, int>();

    //        if (!_repeatedParameterCounts.TryGetValue(currentName, out var currentCount))
    //        {
    //            _repeatedParameterCounts[currentName] = 0;

    //            return currentName;
    //        }

    //        currentCount++;
    //        _repeatedParameterCounts[currentName] = currentCount;

    //        return currentName + "_" + currentCount;
    //    }
    //}
}

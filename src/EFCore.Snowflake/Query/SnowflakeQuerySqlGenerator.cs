using EFCore.Snowflake.Storage.Internal;
using EFCore.Snowflake.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

namespace EFCore.Snowflake.Query;

public class SnowflakeQuerySqlGenerator : QuerySqlGenerator
{
    private static readonly Dictionary<ExpressionType, string> binaryFunctions = new()
    {
        { ExpressionType.And, "BITAND" },
        { ExpressionType.Or, "BITOR" }
    };

    private readonly SnowflakeSqlGenerationHelper _sqlGenerationHelper;

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

    protected override Expression VisitCollate(CollateExpression collateExpression)
    {
        Visit(collateExpression.Operand);

        Sql.Append(" COLLATE '")
            .Append(SnowflakeStringLikeEscape.EscapeSqlLiteral(collateExpression.Collation))
            .Append("'");

        return collateExpression;
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

    private readonly HashSet<string> _parameterNames = [];
    protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
    {
        var name = sqlParameterExpression.Name;

        // Only add the parameter to the command the first time we see its (non-invariant) name, even though we may need to add its
        // placeholder multiple times.
        if (!_parameterNames.Contains(name))
        {
            Sql.AddParameter(
                sqlParameterExpression.InvariantName,
                _sqlGenerationHelper.GenerateParameterName(name),
                sqlParameterExpression.TypeMapping!,
                sqlParameterExpression.IsNullable);
            _parameterNames.Add(name);
        }

        Sql.Append(_sqlGenerationHelper.GenerateParameterNamePlaceholder(name, sqlParameterExpression.TypeMapping));

        return sqlParameterExpression;
    }
}

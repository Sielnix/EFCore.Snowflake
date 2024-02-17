using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Query;

public class SnowflakeSqlExpressionFactory : SqlExpressionFactory
{
    public SnowflakeSqlExpressionFactory(SqlExpressionFactoryDependencies dependencies)
        : base(dependencies)
    {
    }

    public override SqlBinaryExpression And(SqlExpression left, SqlExpression right, RelationalTypeMapping? typeMapping = null)
    {
        //SqlBinaryExpression baseResult = base.And(left, right, typeMapping);
        //RelationalTypeMapping? inferredTypeMapping = typeMapping ?? ExpressionExtensions.InferTypeMapping(left, right);
        //Type resultType = inferredTypeMapping?.ClrType ?? left.Type;
        //RelationalTypeMapping? resultTypeMapping = inferredTypeMapping;

        //return Function(
        //    name: "BITAND",
        //    arguments: new[]
        //    {
        //        baseResult.Left,
        //        baseResult.Right
        //    },
        //    nullable: true,
        //    argumentsPropagateNullability: Statics.TrueArrays[2],
        //    returnType: baseResult.Type,
        //    typeMapping: baseResult.TypeMapping);
        
        return base.And(left, right, typeMapping);
    }
}

using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Snowflake.Query.ExpressionTranslators;

public class SnowflakeAggregateMethodCallTranslatorProvider : RelationalAggregateMethodCallTranslatorProvider
{
    public SnowflakeAggregateMethodCallTranslatorProvider(RelationalAggregateMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        ISqlExpressionFactory sqlExpressionFactory = dependencies.SqlExpressionFactory;

        AddTranslators(new IAggregateMethodCallTranslator[]
        {
            new SnowflakeStringAggregateMethodTranslator(sqlExpressionFactory)
        });
    }
}

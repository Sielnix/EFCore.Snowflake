using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Snowflake.Query.ExpressionTranslators;

public class SnowflakeMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public SnowflakeMethodCallTranslatorProvider(RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        ISqlExpressionFactory sqlExpressionFactory = dependencies.SqlExpressionFactory;

        AddTranslators(new IMethodCallTranslator[]
        {
            new SnowflakeByteArrayMethodTranslator(sqlExpressionFactory),
            new SnowflakeDateOnlyMethodTranslator(sqlExpressionFactory),
            new SnowflakeDateTimeMethodTranslator(sqlExpressionFactory),
            new SnowflakeMathTranslator(sqlExpressionFactory),
            new SnowflakeStringMethodTranslator(sqlExpressionFactory),
            new SnowflakeTimeOnlyMethodTranslator(sqlExpressionFactory)
        });
    }
}

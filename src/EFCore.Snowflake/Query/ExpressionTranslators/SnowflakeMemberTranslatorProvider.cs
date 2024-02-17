using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Query.ExpressionTranslators;

public class SnowflakeMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public SnowflakeMemberTranslatorProvider(
        RelationalMemberTranslatorProviderDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
        : base(dependencies)
    {
        ISqlExpressionFactory sqlExpressionFactory = dependencies.SqlExpressionFactory;
        AddTranslators(new IMemberTranslator[]
        {
            new SnowflakeDateTimeMemberTranslator(typeMappingSource, sqlExpressionFactory),
            new SnowflakeStringMemberTranslator(sqlExpressionFactory)
        });
    }
}

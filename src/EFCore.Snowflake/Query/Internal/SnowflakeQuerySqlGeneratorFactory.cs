using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Snowflake.Query.Internal;
internal class SnowflakeQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _dependencies;

    public SnowflakeQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies)
    {
        _dependencies = dependencies;
    }


    public QuerySqlGenerator Create()
    {
        return new SnowflakeQuerySqlGenerator(_dependencies);
    }
}

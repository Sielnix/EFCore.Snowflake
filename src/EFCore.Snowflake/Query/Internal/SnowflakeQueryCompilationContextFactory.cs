using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using System.Diagnostics.CodeAnalysis;

namespace EFCore.Snowflake.Query.Internal;

public class SnowflakeQueryCompilationContextFactory : RelationalQueryCompilationContextFactory
{
    public SnowflakeQueryCompilationContextFactory(QueryCompilationContextDependencies dependencies, RelationalQueryCompilationContextDependencies relationalDependencies) : base(dependencies, relationalDependencies)
    {
    }

    public override QueryCompilationContext Create(bool async)
    {
        return new SnowflakeQueryCompilationContext(Dependencies, RelationalDependencies, async);
    }
}

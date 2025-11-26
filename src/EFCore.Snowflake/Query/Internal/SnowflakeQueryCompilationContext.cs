using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Snowflake.Query.Internal;

public class SnowflakeQueryCompilationContext : RelationalQueryCompilationContext
{
    public SnowflakeQueryCompilationContext(QueryCompilationContextDependencies dependencies, RelationalQueryCompilationContextDependencies relationalDependencies, bool async)
        : base(dependencies, relationalDependencies, async)
    {
    }
}

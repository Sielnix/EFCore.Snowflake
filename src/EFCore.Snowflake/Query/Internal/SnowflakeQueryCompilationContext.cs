using Microsoft.EntityFrameworkCore.Query;
using System.Diagnostics.CodeAnalysis;

namespace EFCore.Snowflake.Query.Internal;

public class SnowflakeQueryCompilationContext : RelationalQueryCompilationContext
{
    public SnowflakeQueryCompilationContext(QueryCompilationContextDependencies dependencies, RelationalQueryCompilationContextDependencies relationalDependencies, bool async)
        : base(dependencies, relationalDependencies, async)
    {
    }

    //[Experimental(EFDiagnostics.PrecompiledQueryExperimental)]
    [Experimental("EF9100")]
    public SnowflakeQueryCompilationContext(QueryCompilationContextDependencies dependencies, RelationalQueryCompilationContextDependencies relationalDependencies, bool async, bool precompiling)
        : base(dependencies, relationalDependencies, async, precompiling)
    {
    }

    public override bool SupportsPrecompiledQuery => true;
}

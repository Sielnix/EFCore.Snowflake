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

    //[Experimental(EFDiagnostics.PrecompiledQueryExperimental)]
    [Experimental("EF9100")]
    public SnowflakeQueryCompilationContext(QueryCompilationContextDependencies dependencies, RelationalQueryCompilationContextDependencies relationalDependencies, bool async, bool precompiling, IReadOnlySet<string>? nonNullableReferenceTypeParameters)
        : base(dependencies, relationalDependencies, async, precompiling, nonNullableReferenceTypeParameters)
    {
    }

    public override bool SupportsPrecompiledQuery => true;
}

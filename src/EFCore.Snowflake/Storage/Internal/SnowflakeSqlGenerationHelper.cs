using System.Text;
using EFCore.Snowflake.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal;

public class SnowflakeSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public SnowflakeSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies)
    {
    }

    public override string GenerateParameterName(string name)
    {
        return name.StartsWith(":", StringComparison.Ordinal)
            ? name
            : ":" + name;
        //return "?";
    }

    public override void GenerateParameterName(StringBuilder builder, string name)
    {
        //builder.Append("?");
        builder.Append(':').Append(name);
    }

    public override string GenerateParameterNamePlaceholder(string name)
    {
        //ThrowWrongGenerateParameterName();
        return base.GenerateParameterNamePlaceholder(name);
    }

    public override void GenerateParameterNamePlaceholder(StringBuilder builder, string name)
    {
        ThrowWrongGenerateParameterName();
        base.GenerateParameterNamePlaceholder(builder, name);
    }

    public virtual string GenerateParameterNamePlaceholder(string name, RelationalTypeMapping? typeMapping)
    {
        if (typeMapping is SnowflakeSemiStructuredTypeMapping semiStructured)
        {
            return $"{semiStructured.InsertWrapFunction}({base.GenerateParameterNamePlaceholder(name)})";
        }

        return base.GenerateParameterNamePlaceholder(name);
    }

    public virtual void GenerateParameterNamePlaceholder(StringBuilder builder, string name, RelationalTypeMapping? typeMapping)
    {
        if (typeMapping is SnowflakeSemiStructuredTypeMapping semiStructured)
        {
            builder.Append(semiStructured.InsertWrapFunction).Append('(');
            base.GenerateParameterNamePlaceholder(builder, name);
            builder.Append(')');
        }
        else
        {
            base.GenerateParameterNamePlaceholder(builder, name);
        }
    }

    private static void ThrowWrongGenerateParameterName()
    {
        throw new InvalidOperationException(
            $"Do not call {nameof(GenerateParameterNamePlaceholder)} method from {nameof(ISqlGenerationHelper)}, use overload in {nameof(SnowflakeSqlGenerationHelper)} with type mapping parameter");
    }
}

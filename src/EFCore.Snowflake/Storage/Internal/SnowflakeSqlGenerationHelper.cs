using EFCore.Snowflake.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using System.Text;

namespace EFCore.Snowflake.Storage.Internal;

public class SnowflakeSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public SnowflakeSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies)
    {
    }

    public override string GenerateParameterName(string name)
    {
        return name.StartsWith(':') ? EscapeDigitStartWithColon(name) : ":" + EscapeDigitStart(name);
    }

    public override void GenerateParameterName(StringBuilder builder, string name)
    {
        builder.Append(':').Append(EscapeDigitStart(name));
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

    // workaround for https://github.com/snowflakedb/snowflake-connector-net/issues/1283
    private string EscapeDigitStartWithColon(string nameWithColon)
    {
        if (nameWithColon.Length <= 1 || !char.IsDigit(nameWithColon[1]))
        {
            return nameWithColon;
        }

        return string.Concat(":x", nameWithColon.AsSpan(1));
    }

    // workaround for https://github.com/snowflakedb/snowflake-connector-net/issues/1283
    private string EscapeDigitStart(string name)
    {
        if (name.Length == 0 || !char.IsDigit(name[0]))
        {
            return name;
        }

        return "x" + name;
    }
}

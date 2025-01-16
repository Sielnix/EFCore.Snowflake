using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public abstract class SnowflakeSemiStructuredTypeMapping : StringTypeMapping, ISnowflakeCustomizedSqlLiteralProvider
{
    protected SnowflakeSemiStructuredTypeMapping(string storeType)
        : this(storeType, typeof(string))
    {
    }

    protected SnowflakeSemiStructuredTypeMapping(string storeType, Type clrType)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(clrType),
                storeType,
                StoreTypePostfix.Size,
                System.Data.DbType.String,
                unicode: true,
                fixedLength: false))
    {
    }

    public virtual string InsertWrapFunction => "PARSE_JSON";

    protected SnowflakeSemiStructuredTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        string stringVal = (string)value;

        return $"{InsertWrapFunction}('{SnowflakeStringLikeEscape.EscapeSqlLiteral(stringVal)}')";
    }

    public virtual string GenerateSqlLiteralForDdl(object? value)
    {
        if (Converter != null)
        {
            value = Converter.ConvertToProvider(value);
        }

        return GenerateProviderValueSqlLiteralForDdl(value);
    }

    public virtual string GenerateProviderValueSqlLiteralForDdl(object? value)
        => value == null
            ? "NULL"
            : GenerateNonNullSqlLiteralForDdl(value);

    protected virtual string GenerateNonNullSqlLiteralForDdl(object value)
    {
        string stringVal = (string)value;

        return $"'{SnowflakeStringLikeEscape.EscapeSqlLiteral(stringVal)}'";
    }
}

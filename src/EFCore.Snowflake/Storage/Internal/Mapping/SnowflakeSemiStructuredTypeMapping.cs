using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public abstract class SnowflakeSemiStructuredTypeMapping : StringTypeMapping
{
    protected SnowflakeSemiStructuredTypeMapping(string storeType)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(string)),
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
}

using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;
internal class SnowflakeStringTypeMapping : StringTypeMapping
{
    public const int MaxSize = 16777216;
    
    private static readonly CaseInsensitiveValueComparer CaseInsensitiveValueComparer = new();

    public SnowflakeStringTypeMapping(int size, string? storeTypeName = null, bool useKeyComparison = false)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(string),
                    comparer: useKeyComparison ? CaseInsensitiveValueComparer : null,
                    keyComparer: useKeyComparison ? CaseInsensitiveValueComparer : null),
                storeTypeName ?? SnowflakeStoreTypeNames.GetVarcharType(size),
                StoreTypePostfix.Size,
                System.Data.DbType.String,
                unicode: true,
                size: size,
                fixedLength: false))
    {
    }

    protected SnowflakeStringTypeMapping(RelationalTypeMappingParameters parameters)
    : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeStringTypeMapping(parameters);
    }

    protected override string EscapeSqlLiteral(string literal)
    {
        return SnowflakeStringLikeEscape.EscapeSqlLiteral(literal);
    }
}

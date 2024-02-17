using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeCharTypeMapping : CharTypeMapping
{
    public new static SnowflakeCharTypeMapping Default { get; } = new(SnowflakeStoreTypeNames.SingleChar);

    public SnowflakeCharTypeMapping(string storeType)
        : this(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(char), jsonValueReaderWriter: JsonCharReaderWriter.Instance),
            storeType,
            StoreTypePostfix.Size,
            System.Data.DbType.StringFixedLength,
            unicode: true,
            fixedLength: true,
            size: 1))
    {
    }

    protected SnowflakeCharTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        char charValue = Convert.ToChar(value);
        
        return SnowflakeStringLikeEscape.GenerateSqlLiteral(charValue);
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeCharTypeMapping(parameters);
    }
}

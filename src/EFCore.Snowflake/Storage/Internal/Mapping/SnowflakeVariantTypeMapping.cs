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

    //public virtual bool RequiresParseJsonWrap => true;
    public virtual string InsertWrapFunction => "PARSE_JSON";

    protected SnowflakeSemiStructuredTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        string stringVal = (string)value;

        return $"{InsertWrapFunction}('{SnowflakeStringLikeEscape.EscapeSqlLiteral(stringVal)}')";
        //if (RequiresParseJsonWrap)
        //{
        //    return $"PARSE_JSON('{SnowflakeStringLikeEscape.EscapeSqlLiteral(stringVal)}')";
        //}

        //return $"'{SnowflakeStringLikeEscape.EscapeSqlLiteral(stringVal)}'";
    }
}

public class SnowflakeVariantTypeMapping : SnowflakeSemiStructuredTypeMapping
{
    public new static readonly SnowflakeVariantTypeMapping Default = new();
    
    public SnowflakeVariantTypeMapping()
        : base(SnowflakeStoreTypeNames.Variant)
    {
    }

    protected SnowflakeVariantTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    //public override bool RequiresParseJsonWrap => false;
    public override string InsertWrapFunction => "TO_VARIANT";

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeVariantTypeMapping(parameters);
    }

    //protected override string EscapeSqlLiteral(string literal)
    //{
    //    return SnowflakeStringLikeEscape.EscapeSqlLiteral(literal);
    //}
}

public class SnowflakeObjectTypeMapping : SnowflakeSemiStructuredTypeMapping
{
    public new static readonly SnowflakeObjectTypeMapping Default = new();

    public SnowflakeObjectTypeMapping()
        : base(SnowflakeStoreTypeNames.Object)
    {
    }

    protected SnowflakeObjectTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeObjectTypeMapping(parameters);
    }

    //protected override string GenerateNonNullSqlLiteral(object value)
    //{
    //    string stringVal = (string)value;

    //    return $"PARSE_JSON('{SnowflakeStringLikeEscape.EscapeSqlLiteral(stringVal)}')";
    //}
}

public class SnowflakeArrayTypeMapping : SnowflakeSemiStructuredTypeMapping
{
    public new static readonly SnowflakeArrayTypeMapping Default = new();

    public SnowflakeArrayTypeMapping()
        : base(SnowflakeStoreTypeNames.Array)
    {
    }

    protected SnowflakeArrayTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeArrayTypeMapping(parameters);
    }

    //protected override string GenerateNonNullSqlLiteral(object value)
    //{
    //    string stringVal = (string)value;

    //    return $"PARSE_JSON('{SnowflakeStringLikeEscape.EscapeSqlLiteral(stringVal)}')";
    //}
}

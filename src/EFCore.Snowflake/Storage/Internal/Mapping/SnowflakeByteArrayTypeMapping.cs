using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeByteArrayTypeMapping : ByteArrayTypeMapping
{
    public new static SnowflakeByteArrayTypeMapping Default { get; } = new(SnowflakeStoreTypeNames.GetBinaryType(null));

    public SnowflakeByteArrayTypeMapping(string storeType)
        : base(storeType)
    {
    }

    protected SnowflakeByteArrayTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeByteArrayTypeMapping(parameters);
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        byte[] byteVal = (byte[])value;

        return FormattableString.Invariant($"TO_BINARY('{Convert.ToHexString(byteVal)}', 'HEX')");
    }
}

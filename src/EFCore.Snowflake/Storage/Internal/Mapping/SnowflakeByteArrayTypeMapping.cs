using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeByteArrayTypeMapping : ByteArrayTypeMapping
{
    public new static SnowflakeByteArrayTypeMapping Default { get; } = new(
        SnowflakeStoreTypeNames.Binary,
        SnowflakeStoreTypeNames.MaxBinarySize);
    
    public SnowflakeByteArrayTypeMapping(
        string storeType,
        int size)
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(byte[]), jsonValueReaderWriter: JsonByteArrayReaderWriter.Instance),
                storeType,
                StoreTypePostfix.Size,
                System.Data.DbType.Binary,
                unicode: false,
                size: size))
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

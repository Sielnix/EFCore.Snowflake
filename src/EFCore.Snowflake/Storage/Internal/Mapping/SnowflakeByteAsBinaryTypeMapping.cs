using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

internal class SnowflakeByteAsBinaryTypeMapping : ByteArrayTypeMapping
{
    public new static SnowflakeByteAsBinaryTypeMapping Default { get; } = new();

    public SnowflakeByteAsBinaryTypeMapping()
        : this(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(byte),
                    converter: new ValueConverterImpl()
                ),
                storeType: SnowflakeStoreTypeNames.GetBinaryType(1),
                dbType: System.Data.DbType.Binary))
    {
    }

    protected SnowflakeByteAsBinaryTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeByteAsBinaryTypeMapping(parameters);
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        byte[] byteVal = (byte[])value;

        return FormattableString.Invariant($"TO_BINARY('{Convert.ToHexString(byteVal)}', 'HEX')");
    }
    private sealed class ValueConverterImpl() : ValueConverter<byte, byte[]>(b => new[] { b }, l => l.Length > 0 ? l[0] : (byte)0);
}

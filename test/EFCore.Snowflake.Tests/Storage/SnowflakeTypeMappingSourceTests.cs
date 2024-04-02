using EFCore.Snowflake.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.Snowflake.Tests.Storage;

public class SnowflakeTypeMappingSourceTests
{
    [Theory]
    [InlineData("NUMBER(38,0)", typeof(long), null, null, null, false, false)]
    [InlineData("NUMBER(10,2)", typeof(decimal), null, 10, 2, false, false)]
    [InlineData("VARCHAR(1)", typeof(char), 1, null, null, true, true)]
    [InlineData("FLOAT", typeof(double), null, null, null, false, false)]
    [InlineData("DATE", typeof(DateOnly), null, null, null, false, false)]
    [InlineData("TIME(9)", typeof(TimeOnly), null, 9, null, false, false)]
    [InlineData("TIMESTAMP_NTZ(9)", typeof(DateTime), null, 9, null, false, false)]
    [InlineData("TIMESTAMP_LTZ(8)", typeof(DateTime), null, 8, null, false, false)]
    [InlineData("TIMESTAMP_TZ(9)", typeof(DateTimeOffset), null, 9, null, false, false)]
    [InlineData("BINARY(1234)", typeof(byte[]), 1234, null, null, false, false)]
    public void By_StoreType(string typeName, Type type, int? size, int? precision, int? scale, bool fixedLength, bool isUnicode)
    {
        SnowflakeTypeMappingSource sut = CreateTypeMappingSource();

        RelationalTypeMapping? mapping = sut.FindMapping(typeName);

        Assert.NotNull(mapping);
        Assert.Same(type, mapping.ClrType);
        Assert.Equal(size, mapping.Size);
        Assert.Equal(precision, mapping.Precision);
        Assert.Equal(scale, mapping.Scale);
        Assert.Equal(isUnicode, mapping.IsUnicode);
        Assert.Equal(fixedLength, mapping.IsFixedLength);
        Assert.Equal(typeName, mapping.StoreType);
    }

    [Theory]
    [InlineData(typeof(DateTime), "TIMESTAMP_NTZ(5)", 5)]
    [InlineData(typeof(DateTime), "DATE", null)]
    [InlineData(typeof(DateTime), "TIME(5)", 5)]
    [InlineData(typeof(TimeOnly), "TIME(8)", 8)]
    [InlineData(typeof(DateOnly), "DATE", null)]
    public void By_Store_AndClrType(Type clrType, string storeType, int? precision)
    {
        RelationalTypeMapping? mapping = CreateTypeMappingSource().FindMapping(clrType, storeType);
        Assert.NotNull(mapping);

        Assert.Same(clrType, mapping.ClrType);
        Assert.Equal(storeType, mapping.StoreType);
        Assert.Equal(precision, mapping.Precision);
    }

    [Theory]
    [InlineData("TIME(6)", "'14:05:06.213456'::TIME(6)")]
    [InlineData("TIME(0)", "'14:05:06'::TIME(0)")]
    [InlineData("DATE", "'2024-01-23'::DATE")]
    [InlineData("TIMESTAMP_LTZ(9)", "'2024-01-23 14:05:06.2134560'::TIMESTAMP_LTZ(9)")]
    public void DateTime_creates_correct_sql_literals(string storeTypeName, string expectedResult)
    {
        RelationalTypeMapping? mapping = CreateTypeMappingSource().FindMapping(typeof(DateTime), storeTypeName);

        Assert.NotNull(mapping);

        string literal = mapping.GenerateProviderValueSqlLiteral(new DateTime(2024, 1, 23, 14, 5, 6, 213, 456));

        Assert.Equal(expectedResult, literal);
    }

    [Theory]
    [InlineData("TIMESTAMP_TZ(9)", "'2024-01-23 14:05:06.2134560+02:00'::TIMESTAMP_TZ(9)")]
    [InlineData("TIMESTAMP_TZ(3)", "'2024-01-23 14:05:06.213+02:00'::TIMESTAMP_TZ(3)")]
    public void DateTimeOffset_creates_correct_sql_literals(string storeTypeName, string expectedResult)
    {
        RelationalTypeMapping? mapping = CreateTypeMappingSource().FindMapping(typeof(DateTimeOffset), storeTypeName);

        Assert.NotNull(mapping);

        DateTime time = new DateTime(2024, 1, 23, 14, 5, 6, 213, 456, DateTimeKind.Unspecified);
        DateTimeOffset timeOffset = new(time, TimeSpan.FromHours(2));

        string literal = mapping.GenerateProviderValueSqlLiteral(timeOffset);

        Assert.Equal(expectedResult, literal);
    }

    private SnowflakeTypeMappingSource CreateTypeMappingSource()
    {
        return new SnowflakeTypeMappingSource(
            new TypeMappingSourceDependencies(
                new ValueConverterSelector(new ValueConverterSelectorDependencies()),
                new JsonValueReaderWriterSource(new JsonValueReaderWriterSourceDependencies()),
                Array.Empty<ITypeMappingSourcePlugin>()),
            new RelationalTypeMappingSourceDependencies(Array.Empty<IRelationalTypeMappingSourcePlugin>()));
    }

}

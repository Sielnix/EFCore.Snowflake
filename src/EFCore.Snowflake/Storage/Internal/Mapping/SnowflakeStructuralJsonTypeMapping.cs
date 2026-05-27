using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeStructuralJsonTypeMapping : SnowflakeSemiStructuredTypeMapping
{
    public new static readonly SnowflakeStructuralJsonTypeMapping Default = new();

    private static readonly MethodInfo GetStringMethod
        = typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetString), [typeof(int)])!;

    private static readonly PropertyInfo UTF8Property
        = typeof(Encoding).GetProperty(nameof(Encoding.UTF8))!;

    private static readonly MethodInfo EncodingGetBytesMethod
        = typeof(Encoding).GetMethod(nameof(Encoding.GetBytes), [typeof(string)])!;

    private static readonly ConstructorInfo MemoryStreamConstructor
        = typeof(MemoryStream).GetConstructor([typeof(byte[])])!;
    
    public SnowflakeStructuralJsonTypeMapping()
        : base(SnowflakeStoreTypeNames.Variant, typeof(JsonTypePlaceholder))
    {
    }

    protected SnowflakeStructuralJsonTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
    {
        return new SnowflakeStructuralJsonTypeMapping(parameters);
    }

    public override MethodInfo GetDataReaderMethod()
        => GetStringMethod;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public override Expression CustomizeDataReaderExpression(Expression expression)
        => Expression.New(
            MemoryStreamConstructor,
            Expression.Call(
                Expression.Property(null, UTF8Property),
                EncodingGetBytesMethod,
                expression));
}

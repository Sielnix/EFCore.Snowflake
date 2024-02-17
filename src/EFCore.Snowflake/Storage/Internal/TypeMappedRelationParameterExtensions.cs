using System.Reflection;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Internal;

namespace EFCore.Snowflake.Storage.Internal;

internal static class TypeMappedRelationParameterExtensions
{
    private const string IsNullablePropertyName = "IsNullable";
    private const string RelationalTypeMappingPropertyName = "RelationalTypeMapping";
    private static readonly MethodInfo RelationalTypeMappingGetter = GetRelationalTypeMappingMethod();
    private static readonly MethodInfo IsNullableGetter = GeIsNullableMethod();

    public static RelationalTypeMapping GetTypeMapping(this TypeMappedRelationalParameter typeMapping)
    {
        object? result = RelationalTypeMappingGetter.Invoke(typeMapping, parameters: null);
        if (result is null)
        {
            throw new InvalidOperationException($"{RelationalTypeMappingPropertyName} is null");
        }

        if (result is not RelationalTypeMapping relationalTypeMapping)
        {
            throw new InvalidOperationException($"{RelationalTypeMappingPropertyName} is not {typeof(RelationalTypeMapping)}");
        }

        return relationalTypeMapping;
    }

    public static bool? GetIsNullable(this TypeMappedRelationalParameter typeMapping)
    {
        object? result = IsNullableGetter.Invoke(typeMapping, parameters: null);
        if (result is null)
        {
            return null;
        }

        if (result is not bool isNullable)
        {
            throw new InvalidOperationException($"{IsNullablePropertyName} is not {typeof(bool?)}");
        }

        return isNullable;
    }

    private static MethodInfo GetRelationalTypeMappingMethod()
    {
        PropertyInfo? property = typeof(TypeMappedRelationalParameter).GetProperty(
            RelationalTypeMappingPropertyName,
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);

        if (property is null)
        {
            throw new InvalidOperationException($"Can not find {RelationalTypeMappingPropertyName}");
        }

        MethodInfo? getMethod = property.GetMethod;
        if (getMethod is null)
        {
            throw new InvalidOperationException($"Can not find get method of {RelationalTypeMappingPropertyName}");
        }

        return getMethod;
    }

    private static MethodInfo GeIsNullableMethod()
    {
        PropertyInfo? property = typeof(TypeMappedRelationalParameter).GetProperty(
            IsNullablePropertyName,
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);

        if (property is null)
        {
            throw new InvalidOperationException($"Can not find {IsNullablePropertyName}");
        }

        MethodInfo? getMethod = property.GetMethod;
        if (getMethod is null)
        {
            throw new InvalidOperationException($"Can not find get method of {IsNullablePropertyName}");
        }

        return getMethod;
    }
}

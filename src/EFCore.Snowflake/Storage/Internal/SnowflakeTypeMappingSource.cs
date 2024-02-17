using EFCore.Snowflake.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage.Internal;

public class SnowflakeTypeMappingSource : RelationalTypeMappingSource
{
    private readonly SnowflakeCharTypeMapping _charTypeMapping = SnowflakeCharTypeMapping.Default;
    private readonly DecimalTypeMapping _decimalTypeMapping = new(SnowflakeStoreTypeNames.DefaultDecimal);
    private readonly ShortTypeMapping _shortTypeMapping = new(SnowflakeStoreTypeNames.GetIntegerTypeToHoldEverything(SignedIntegerType.Short));
    private readonly IntTypeMapping _intTypeMapping = new(SnowflakeStoreTypeNames.GetIntegerTypeToHoldEverything(SignedIntegerType.Int));
    private readonly LongTypeMapping _longTypeMapping = new(SnowflakeStoreTypeNames.GetIntegerTypeToHoldEverything(SignedIntegerType.Long));
    private readonly DoubleTypeMapping _doubleTypeMapping = new(SnowflakeStoreTypeNames.Float);
    private readonly FloatTypeMapping _floatTypeMapping = new(SnowflakeStoreTypeNames.Float);

    private readonly Dictionary<Type, RelationalTypeMapping> _clrTypeMappings;

    private readonly Dictionary<string, RelationalTypeMapping[]> _storeTypeMappings;

    private readonly Dictionary<string, RelationalTypeMapping[]> _storeTimeTypeMappings;

    private readonly RelationalTypeMapping[] _integerNumberTypeMappings;
    private readonly RelationalTypeMapping[] _rationalNumberTypeMappings;

    public SnowflakeTypeMappingSource(TypeMappingSourceDependencies dependencies, RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
        _clrTypeMappings = new Dictionary<Type, RelationalTypeMapping>
        {
            { typeof(bool), SnowflakeBoolTypeMapping.Default },
            { typeof(char), _charTypeMapping },
            { typeof(byte), SnowflakeByteAsBinaryTypeMapping.Default },
            { typeof(sbyte), SnowflakeSByteTypeMapping.Default },
            { typeof(short), _shortTypeMapping},
            { typeof(ushort), SnowflakeUShortTypeMapping.Default},
            { typeof(int), _intTypeMapping },
            { typeof(uint), SnowflakeUIntTypeMapping.Default },
            { typeof(long), _longTypeMapping},
            { typeof(ulong), SnowflakeULongTypeMapping.Default},
            { typeof(decimal), _decimalTypeMapping},
            { typeof(double), _doubleTypeMapping },
            { typeof(float), _floatTypeMapping },
            { typeof(DateOnly), SnowflakeDateOnlyTypeMapping.Default },
            { typeof(TimeOnly), SnowflakeTimeOnlyTypeMapping.Default },
            { typeof(DateTime), SnowflakeDateTimeTypeMapping.Default },
            { typeof(DateTimeOffset), SnowflakeDateTimeOffsetTypeMapping.Default },
            { typeof(byte[]), SnowflakeByteArrayTypeMapping.Default },
        };

        _storeTypeMappings = new Dictionary<string, RelationalTypeMapping[]>
        {
            { SnowflakeStoreTypeNames.Boolean, [ SnowflakeBoolTypeMapping.Default ] },
            { SnowflakeStoreTypeNames.Date, [ SnowflakeDateOnlyTypeMapping.Default, new SnowflakeDateTimeTypeMapping(SnowflakeStoreTypeNames.Date, precision: null) ] },
            { SnowflakeStoreTypeNames.SingleChar, [ _charTypeMapping, new SnowflakeStringTypeMapping(1) ]},
            { SnowflakeStoreTypeNames.SingleByte, [ SnowflakeByteAsBinaryTypeMapping.Default, SnowflakeByteAsIntTypeMapping.Default ]},
            { SnowflakeStoreTypeNames.Float, [ _doubleTypeMapping, _floatTypeMapping ]},
            { SnowflakeStoreTypeNames.Variant, [ SnowflakeVariantTypeMapping.Default ] },
            { SnowflakeStoreTypeNames.Array, [ SnowflakeArrayTypeMapping.Default ] },
            { SnowflakeStoreTypeNames.Object, [ SnowflakeObjectTypeMapping.Default ] }
        };

        _storeTimeTypeMappings = new Dictionary<string, RelationalTypeMapping[]>
        {
            {
                SnowflakeStoreTypeNames.Time,
                [
                    SnowflakeTimeOnlyTypeMapping.Default,
                    new SnowflakeDateTimeTypeMapping(
                        SnowflakeStoreTypeNames.GetTimeType(SnowflakeStoreTypeNames.Time, SnowflakeStoreTypeNames.DefaultTimePrecision),
                        SnowflakeStoreTypeNames.DefaultTimePrecision)
                ]
            },
            { SnowflakeStoreTypeNames.TimestampNtz, [ SnowflakeDateTimeTypeMapping.Default ] },
            { SnowflakeStoreTypeNames.TimestampLtz, [ SnowflakeDateTimeTypeMapping.Default ] },
            { SnowflakeStoreTypeNames.TimestampTz, [ SnowflakeDateTimeOffsetTypeMapping.Default ] },
        };

        _integerNumberTypeMappings = [
            SnowflakeByteAsIntTypeMapping.Default,
            SnowflakeSByteTypeMapping.Default,
            _shortTypeMapping,
            SnowflakeUShortTypeMapping.Default,
            _intTypeMapping,
            SnowflakeUIntTypeMapping.Default,
            _longTypeMapping,
            SnowflakeULongTypeMapping.Default,
            _decimalTypeMapping,
            _doubleTypeMapping,
            _floatTypeMapping
        ];

        _rationalNumberTypeMappings = [
            _decimalTypeMapping,
            _doubleTypeMapping,
            _floatTypeMapping
        ];
    }

    //private bool isSearching = false;
    //private bool wasCalled = false;

    //private const string searchCol = "timeOnlyCol";

    //public override CoreTypeMapping? FindMapping(IProperty property)
    //{
    //    if (property.Name == searchCol)
    //    {
    //        isSearching = true;
    //        FindMappingBase(property);
    //    }

    //    var result = base.FindMapping(property);

    //    //if (wasCalled)
    //    //{
    //    //    wasCalled = false;

    //    //    int i = 1;
    //    //    foreach (var principal in property.GetPrincipals())
    //    //    {

    //    //        foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(principal))
    //    //        {
    //    //            Console.WriteLine($"PRINCIPAL {i}");
    //    //            string name = descriptor.Name;
    //    //            object value = descriptor.GetValue(principal);
    //    //            Console.WriteLine("{0}={1}", name, value);
    //    //            i++;
    //    //        }
    //    //    }
    //    //}

    //    isSearching = false;
    //    if (property.Name == searchCol)
    //    {
    //        Console.WriteLine($"looking for mapping for property {property}, name {property.Name}, result is NUll {result == null}");
    //    }


    //    return result;
    //}

    //public CoreTypeMapping? FindMappingBase(IProperty property)
    //{
    //    var principals = property.GetPrincipals();

    //    string? storeTypeName = null;
    //    bool? isFixedLength = null;
    //    // ReSharper disable once ForCanBeConvertedToForeach
    //    for (var i = 0; i < principals.Count; i++)
    //    {
    //        var principal = principals[i];
    //        if (storeTypeName == null)
    //        {
    //            var columnType = (string?)principal[RelationalAnnotationNames.ColumnType];
    //            if (columnType != null)
    //            {
    //                storeTypeName = columnType;
    //            }
    //        }

    //        isFixedLength ??= principal.IsFixedLength();
    //    }

    //    bool? unicode = null;
    //    int? size = null;
    //    int? precision = null;
    //    int? scale = null;
    //    var storeTypeNameBase = ParseStoreTypeName(storeTypeName, ref unicode, ref size, ref precision, ref scale);

    //    return FindMappingWithConversion(
    //        new RelationalTypeMappingInfo(principals, storeTypeName, storeTypeNameBase, unicode, isFixedLength, size, precision, scale),
    //        principals);
    //}


    //private RelationalTypeMapping? FindMappingWithConversion(
    //    RelationalTypeMappingInfo mappingInfo,
    //    IReadOnlyList<IProperty>? principals)
    //{
    //    Type? providerClrType = null;
    //    ValueConverter? customConverter = null;
    //    CoreTypeMapping? elementMapping = null;
    //    if (principals != null)
    //    {
    //        for (var i = 0; i < principals.Count; i++)
    //        {
    //            var principal = principals[i];
    //            if (providerClrType == null)
    //            {
    //                var providerType = principal.GetProviderClrType();
    //                if (providerType != null)
    //                {
    //                    providerClrType = providerType.UnwrapNullableType();
    //                }
    //            }

    //            if (customConverter == null)
    //            {
    //                var converter = principal.GetValueConverter();
    //                if (converter != null)
    //                {
    //                    customConverter = converter;
    //                }
    //            }

    //            if (elementMapping == null)
    //            {
    //                var element = principal.GetElementType();
    //                if (element != null)
    //                {
    //                    elementMapping = FindMapping(element);
    //                    mappingInfo = mappingInfo with { ElementTypeMapping = (RelationalTypeMapping?)elementMapping };
    //                }
    //            }
    //        }
    //    }

    //    //var resolvedMapping = FindMappingWithConversion(mappingInfo, providerClrType, customConverter);
    //    PrintObject(mappingInfo);
    //    PrintObject(providerClrType);
    //    PrintObject(customConverter);
    //    PrintObject(mappingInfo.ElementTypeMapping);

    //    //ValidateMapping(resolvedMapping, principals?[0]);

    //    return null;
    //}

    //private void PrintObject<T>(T? obj, [CallerArgumentExpression(nameof(obj))] string? expr = null)
    //{
    //    Console.WriteLine($"Object {expr}");
    //    if (obj == null)
    //    {
    //        Console.WriteLine("IS NULL");
    //        return;
    //    }

    //    foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(obj))
    //    {
    //        string name = descriptor.Name;
    //        object value = descriptor.GetValue(obj);
    //        Console.WriteLine("{0}={1}", name, value);
    //    }
    //}

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        DoLog(mappingInfo);

        RelationalTypeMapping? result = base.FindMapping(mappingInfo);

        if (result == null)
        {
            result = FindRawMapping(in mappingInfo);
        }

        return result;
    }

    private static HashSet<string> FoundTypeNames = new();

    private void DoLog(in RelationalTypeMappingInfo mappingInfo)
    {
        //if (isSearching)
        //{
        //    foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(mappingInfo))
        //    {
        //        string name = descriptor.Name;
        //        object value = descriptor.GetValue(mappingInfo);
        //        Console.WriteLine("{0}={1}", name, value);
        //    }

        //    wasCalled = true;
        //}



        //bool found = false;
        //if (mappingInfo.StoreTypeName is not null && FoundTypeNames.Add(mappingInfo.StoreTypeName))
        //{
        //    found = true;
        //}

        //if (mappingInfo.StoreTypeNameBase is not null && FoundTypeNames.Add(mappingInfo.StoreTypeNameBase))
        //{
        //    found = true;
        //}

        //if (found)
        //{
        //    File.WriteAllLines("C:\\repo\\out.txt", FoundTypeNames);
        //}

        if (mappingInfo.ClrType is null)
        {
            Console.WriteLine(PrettyPrint(mappingInfo));
        }

    }

    private string PrettyPrint<T>(T? obj)
    {
        if (obj is null)
        {
            return "NULL";
        }

        return System.Text.Json.JsonSerializer.Serialize(obj);
    }

    private RelationalTypeMapping? FindRawMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        Type? clrType = mappingInfo.ClrType;
        string? storeTypeName = mappingInfo.StoreTypeName;
        string? storeTypeNameBase = mappingInfo.StoreTypeNameBase;

        if (clrType == typeof(byte[]) && mappingInfo.ElementTypeMapping != null)
        {
            return null;
        }

        if (storeTypeName is not null)
        {
            if (storeTypeNameBase is null)
            {
                throw new ArgumentNullException(
                    nameof(storeTypeNameBase),
                    @$"Value of {nameof(storeTypeNameBase)} shouldn't be null when {nameof(storeTypeName)} is not null");
            }

            if (_storeTypeMappings.TryGetValue(storeTypeName, out RelationalTypeMapping[]? mapping))
            {
                return clrType is null
                    ? mapping[0]
                    : mapping.FirstOrDefault(m => m.ClrType == clrType);
            }

            if (_storeTimeTypeMappings.TryGetValue(storeTypeNameBase, out mapping))
            {
                RelationalTypeMapping? mappingType = clrType is null
                    ? mapping[0]
                    : mapping.FirstOrDefault(m => m.ClrType == clrType);

                return mappingType?
                    .WithStoreTypeAndSize(storeTypeName, null)
                    .WithPrecisionAndScale(precision: mappingInfo.Size, null);
            }

            if (storeTypeNameBase == SnowflakeStoreTypeNames.Binary)
            {
                return new ByteArrayTypeMapping(storeTypeName, size: mappingInfo.Size);
            }

            if (storeTypeNameBase == SnowflakeStoreTypeNames.Varchar)
            {
                if (clrType is not null && clrType != typeof(string))
                {
                    return null;
                }

                return GetStringMapping(storeTypeName, mappingInfo.Size);
            }

            if (storeTypeNameBase == SnowflakeStoreTypeNames.Number)
            {
                if (clrType is null)
                {
                    bool isInteger = !mappingInfo.Scale.HasValue || mappingInfo.Scale.Value == 0;
                    if (isInteger)
                    {
                        SignedIntegerType integerType = SnowflakeStoreTypeNames.GetSafeIntegerType(mappingInfo.Precision);
                        return GetIntegerTypeMapping(storeTypeName, integerType);
                    }

                    if (storeTypeName == _decimalTypeMapping.StoreType)
                    {
                        return _decimalTypeMapping;
                    }

                    return new DecimalTypeMapping(
                        storeTypeName,
                        precision: mappingInfo.Precision,
                        scale: mappingInfo.Scale);
                }

                if (!mappingInfo.Scale.HasValue || mappingInfo.Scale.Value == 0)
                {
                    RelationalTypeMapping? type = _integerNumberTypeMappings.FirstOrDefault(t => t.ClrType == clrType);

                    return type?
                        .WithPrecisionAndScale(mappingInfo.Precision, mappingInfo.Scale)
                        .WithStoreTypeAndSize(storeTypeName, size: null);
                }
                else
                {
                    RelationalTypeMapping? type = _rationalNumberTypeMappings.FirstOrDefault(t => t.ClrType == clrType);
                    return type?
                        .WithPrecisionAndScale(mappingInfo.Precision, mappingInfo.Scale)
                        .WithStoreTypeAndSize(storeTypeName, size: null);
                }
            }
        }

        //if (clrType != null && clrType.IsGenericType && clrType.GetGenericTypeDefinition() == typeof(Nullable<>))
        //{
        //    int i = 5;
        //}

        if (clrType != null)
        {
            if (_clrTypeMappings.TryGetValue(clrType, out RelationalTypeMapping? clrTypeMapping))
            {
                return clrTypeMapping;
            }

            if (clrType == typeof(string))
            {
                return GetStringMapping(storeTypeName, mappingInfo.Size);
            }
        }

        return null;
    }

    private RelationalTypeMapping GetIntegerTypeMapping(string storeTypeName, SignedIntegerType type)
    {
        switch (type)
        {
            case SignedIntegerType.Byte:
                return new SnowflakeByteAsIntTypeMapping(storeTypeName);
            case SignedIntegerType.Short:
                return new ShortTypeMapping(storeTypeName);
            case SignedIntegerType.Int:
                return new IntTypeMapping(storeTypeName);
            case SignedIntegerType.Long:
                return new LongTypeMapping(storeTypeName);
            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, null);
        }
    }

    private SnowflakeStringTypeMapping GetStringMapping(string? storeTypeName, int? size)
    {
        const int maxSize = SnowflakeStringTypeMapping.MaxSize;
        int resultSize = maxSize;
        if (size is > 0 and <= maxSize)
        {
            resultSize = size.Value;
        }

        return new SnowflakeStringTypeMapping(resultSize, storeTypeName);
    }
}

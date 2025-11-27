using EFCore.Snowflake.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System.Text.Json;

namespace EFCore.Snowflake.Storage.Internal;

public class SnowflakeTypeMappingSource : RelationalTypeMappingSource
{
    private readonly SnowflakeCharTypeMapping _charTypeMapping = SnowflakeCharTypeMapping.Default;
    private readonly DecimalTypeMapping _decimalTypeMapping = SnowflakeDecimalTypeMapping.Default;
    private readonly SnowflakeShortTypeMapping _shortTypeMapping = SnowflakeShortTypeMapping.Default;
    private readonly SnowflakeIntTypeMapping _intTypeMapping = SnowflakeIntTypeMapping.Default;
    private readonly SnowflakeLongTypeMapping _longTypeMapping = SnowflakeLongTypeMapping.Default;
    private readonly DoubleTypeMapping _doubleTypeMapping = new(SnowflakeStoreTypeNames.Float);
    private readonly FloatTypeMapping _floatTypeMapping = new(SnowflakeStoreTypeNames.Float);

    private readonly SnowflakeDoubleAsNumberTypeMapping _doubleAsNumberTypeMapping = SnowflakeDoubleAsNumberTypeMapping.Default;
    private readonly SnowflakeFloatAsNumberTypeMapping _floatAsNumberTypeMapping = SnowflakeFloatAsNumberTypeMapping.Default;

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
            { typeof(JsonElement), SnowflakeJsonTypeMapping.Default },
            { typeof(JsonTypePlaceholder), SnowflakeStructuralJsonTypeMapping.Default }
        };

        _storeTypeMappings = new Dictionary<string, RelationalTypeMapping[]>(SnowflakeStoreTypeNames.TypeNameComparer)
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

        ExtendWithAliases(_storeTypeMappings);

        _storeTimeTypeMappings = new Dictionary<string, RelationalTypeMapping[]>(SnowflakeStoreTypeNames.TypeNameComparer)
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
            { SnowflakeStoreTypeNames.TimestampLtz, [ SnowflakeDateTimeOffsetAsLtzTypeMapping.Default, SnowflakeDateTimeAsLtzTypeMapping.Default ] },
            { SnowflakeStoreTypeNames.TimestampTz, [ SnowflakeDateTimeOffsetTypeMapping.Default ] },
        };

        ExtendWithAliases(_storeTimeTypeMappings);

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
            _doubleAsNumberTypeMapping,
            _floatAsNumberTypeMapping
        ];

        _rationalNumberTypeMappings = [
            _decimalTypeMapping,
            _doubleAsNumberTypeMapping,
            _floatAsNumberTypeMapping
        ];
    }

    private static void ExtendWithAliases<T>(Dictionary<string, T> mappings)
    {
        foreach (var aliasTypeNames in SnowflakeStoreTypeNames.AliasTypeNames)
        {
            if (mappings.TryGetValue(aliasTypeNames.Key, out T? mappingInfo))
            {
                foreach (string aliasTypeName in aliasTypeNames.Value)
                {
                    mappings.Add(aliasTypeName, mappingInfo);
                }
            }
        }
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        RelationalTypeMapping? result = base.FindMapping(mappingInfo);

        if (result == null)
        {
            result = FindRawMapping(in mappingInfo);
        }

        return result;
    }

    protected override RelationalTypeMapping? FindCollectionMapping(
        RelationalTypeMappingInfo info,
        Type modelType,
        Type? providerType,
        CoreTypeMapping? elementMapping)
    {
        return TryFindJsonCollectionMapping(
            info.CoreTypeMappingInfo, modelType, providerType, ref elementMapping, out var comparer, out var collectionReaderWriter)
            // only change against original one - we use SnowflakeArrayTypeMapping directly instead of searching for string type mapping
            ? (RelationalTypeMapping)SnowflakeArrayTypeMapping.Default
                .WithComposedConverter(
                    (ValueConverter)Activator.CreateInstance(
                        typeof(CollectionToJsonStringConverter<>).MakeGenericType(
                            modelType.TryGetElementType(typeof(IEnumerable<>))!), collectionReaderWriter!)!,
                    comparer,
                    comparer,
                    elementMapping,
                    collectionReaderWriter)
            : null;
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

        //if (clrType == typeof(JsonElement) || clrType == typeof(JsonDocument))
        //{
        //    return null;
        //}

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
                    .WithPrecisionAndScale(
                        precision: mappingInfo.Precision ?? mappingInfo.Size ?? mappingType.Precision,
                        scale: null);
            }

            if (SnowflakeStoreTypeNames.TypeNameComparer.Equals(storeTypeNameBase, SnowflakeStoreTypeNames.Binary))
            {
                return new ByteArrayTypeMapping(storeTypeName, size: mappingInfo.Size);
            }

            if (SnowflakeStoreTypeNames.TypeNameComparer.Equals(storeTypeNameBase, SnowflakeStoreTypeNames.Varchar))
            {
                if (clrType is not null && clrType != typeof(string))
                {
                    return null;
                }

                return GetStringMapping(storeTypeName, mappingInfo.Size);
            }

            if (SnowflakeStoreTypeNames.FixedPointNumberTypeNames.Contains(storeTypeNameBase))
            {
                bool isInteger = (!mappingInfo.Scale.HasValue || mappingInfo.Scale.Value == 0)
                                 || (!mappingInfo.Scale.HasValue && !mappingInfo.Precision.HasValue)
                                 || SnowflakeStoreTypeNames.IntegerTypeNames.Contains(storeTypeNameBase);

                int? integerPrecision = mappingInfo.Precision ?? mappingInfo.Size;

                if (clrType is null)
                {
                    if (isInteger)
                    {
                        SignedIntegerType integerType = SnowflakeStoreTypeNames.GetSafeIntegerType(integerPrecision);
                        return GetIntegerTypeMapping(mappingInfo.Precision, integerType);
                    }

                    return new SnowflakeDecimalTypeMapping(
                        storeTypeName,
                        precision: mappingInfo.Precision,
                        scale: mappingInfo.Scale);
                }

                if (isInteger)
                {
                    RelationalTypeMapping? type = _integerNumberTypeMappings.FirstOrDefault(t => t.ClrType == clrType);

                    if (type is null)
                    {
                        return null;
                    }

                    switch (type.StoreTypePostfix)
                    {
                        case StoreTypePostfix.None:
                            return type;
                        case StoreTypePostfix.Size:
                            return type.WithStoreTypeAndSize(type.StoreType, mappingInfo.Size ?? type.Size);
                        case StoreTypePostfix.Precision:
                            return type.WithPrecisionAndScale(
                                integerPrecision,
                                type.Scale);
                        case StoreTypePostfix.PrecisionAndScale:
                            return type.WithPrecisionAndScale(
                                mappingInfo.Precision ?? type.Precision,
                                mappingInfo.Scale ?? type.Scale);
                        default:
                            return null;
                    }
                }
                else
                {
                    RelationalTypeMapping? type = _rationalNumberTypeMappings.FirstOrDefault(t => t.ClrType == clrType);
                    return type?
                        .WithPrecisionAndScale(mappingInfo.Precision, mappingInfo.Scale);
                }
            }
        }

        if (clrType != null)
        {
            if (_clrTypeMappings.TryGetValue(clrType, out RelationalTypeMapping? clrTypeMapping))
            {
                RelationalTypeMapping mapping = clrTypeMapping;

                int? size;
                int? precision;
                int? scale = mappingInfo.Scale ?? mapping.Scale;

                if (mapping.StoreTypePostfix == StoreTypePostfix.Precision)
                {
                    // efcore gives us precision as size instead of precision when StoreTypePostfix is Precision
                    size = mapping.Size;
                    precision = mappingInfo.Precision ?? mappingInfo.Size ?? mapping.Precision;
                }
                else
                {
                    size = mappingInfo.Size ?? mapping.Size;
                    precision = mappingInfo.Precision ?? mapping.Precision;
                }

                return mapping
                    .WithPrecisionAndScale(precision, scale)
                    .WithStoreTypeAndSize(mapping.StoreType, size);
            }

            if (clrType == typeof(string))
            {
                return GetStringMapping(storeTypeName, mappingInfo.Size);
            }

            if (clrType == typeof(byte[]) && mappingInfo.ElementTypeMapping == null)
            {
                return SnowflakeByteArrayTypeMapping.Default.WithTypeMappingInfo(mappingInfo);
            }
        }

        return null;
    }

    private RelationalTypeMapping GetIntegerTypeMapping(int? precision, SignedIntegerType type)
    {
        switch (type)
        {
            case SignedIntegerType.Byte:
                return new SnowflakeByteAsIntTypeMapping(precision);
            case SignedIntegerType.Short:
                return new SnowflakeShortTypeMapping(precision);
            case SignedIntegerType.Int:
                return new SnowflakeIntTypeMapping(precision);
            case SignedIntegerType.Long:
                return new SnowflakeLongTypeMapping(precision);
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

using EFCore.Snowflake.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

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

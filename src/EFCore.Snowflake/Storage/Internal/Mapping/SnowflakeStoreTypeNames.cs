namespace EFCore.Snowflake.Storage.Internal.Mapping;

internal static class SnowflakeStoreTypeNames
{
    public const string SingleChar = "VARCHAR(1)";
    public const string SingleByte = "BINARY(1)";
    public const string DefaultDecimal = "NUMBER(38,18)";
    public const string Float = "FLOAT";
    public const string Boolean = "BOOLEAN";
    public const string Date = "DATE";

    public const string Varchar = "VARCHAR";
    public const string Number = "NUMBER";
    public const string Binary = "BINARY";

    public const string Variant = "VARIANT";
    public const string Array = "ARRAY";
    public const string Object = "OBJECT";

    public const string Time = "TIME";
    public const string TimestampNtz = "TIMESTAMP_NTZ";
    public const string TimestampLtz = "TIMESTAMP_LTZ";
    public const string TimestampTz = "TIMESTAMP_TZ";

    public const int MaxByteFullDecimalDigits = 2;
    public const int MaxShortFullDecimalDigits = 4;
    public const int MaxIntFullDecimalDigits = 9;
    public const int MaxLongFullDecimalDigits = 18;

    public const int MaxNumberSize = 38;
    public const int DefaultScaleForDecimal = 18;

    public const int DefaultTimePrecision = 9;

    public const int MaxBinarySize = 8_388_608;

    public const int MaxDotNetDateTimePrecision = 7;

    public static string GetIntegerTypeToHoldEverything(SignedIntegerType type)
    {
        int maxFullyHoldable = GetMaxFullHoldableDecimalDigits(type);
        int requiredToStoreAllOptions = maxFullyHoldable + 1;

        return GetIntegerType(requiredToStoreAllOptions);
    }

    public static SignedIntegerType GetSafeIntegerType(int? decimals)
    {
        if (!decimals.HasValue)
        {
            return SignedIntegerType.Long;
        }

        SignedIntegerType[] signedIntegerTypes = Enum.GetValues<SignedIntegerType>();
        foreach (var signedIntegerType in signedIntegerTypes)
        {
            int fullyHoldable = GetMaxFullHoldableDecimalDigits(signedIntegerType);
            if (decimals.Value <= fullyHoldable)
            {
                return signedIntegerType;
            }
        }

        return SignedIntegerType.Long;
    }

    private static int GetMaxFullHoldableDecimalDigits(SignedIntegerType type)
    {
        switch (type)
        {
            case SignedIntegerType.Byte:
                return MaxByteFullDecimalDigits;
            case SignedIntegerType.Short:
                return MaxShortFullDecimalDigits;
            case SignedIntegerType.Int:
                return MaxIntFullDecimalDigits;
            case SignedIntegerType.Long:
                return MaxLongFullDecimalDigits;
            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, null);
        }
    }

    public static string GetIntegerType(int? precision)
    {
        if (!precision.HasValue)
        {
            precision = MaxNumberSize;
        }

        return GetDecimalType(precision, scale: 0);
    }

    public static string GetVarcharType(int size)
    {
        return FormattableString.Invariant($"VARCHAR({size})");
    }

    public static string GetDecimalType(int? precision, int? scale)
    {
        return FormattableString.Invariant($"NUMBER({precision ?? MaxNumberSize},{scale ?? DefaultScaleForDecimal})");
    }

    public static string GetTimeType(string typeNameBase, int precision)
    {
        const int minPrecision = 0;
        const int maxPrecision = 9;
        if (precision < minPrecision || precision > maxPrecision)
        {
            throw new ArgumentOutOfRangeException(
                nameof(precision),
                precision,
                @$"Time precision must be within {minPrecision}-{maxPrecision}, but {precision} was provided");
        }

        return FormattableString.Invariant($"{typeNameBase}({precision})");
    }

    public static string GetBinaryType(int? length)
    {
        return FormattableString.Invariant($"BINARY({length ?? MaxBinarySize})");
    }
}

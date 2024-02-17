namespace EFCore.Snowflake.Storage.Internal.Mapping;

internal enum SignedIntegerType
{
    // ensure order is in type max length ascending
    Byte,
    Short,
    Int,
    Long
}

namespace EFCore.Snowflake.Utilities;
internal static class Statics
{
    internal static readonly bool[][] TrueArrays =
    {
        Array.Empty<bool>(),
        [true],
        [true, true],
        [true, true, true],
        [true, true, true, true],
        [true, true, true, true, true],
        [true, true, true, true, true, true],
        [true, true, true, true, true, true, true],
        [true, true, true, true, true, true, true, true]
    };

    internal static readonly bool[][] FalseArrays = { Array.Empty<bool>(), [false], [false, false] };
}

namespace EFCore.Snowflake.Extensions;
internal static class EnumerableExtensions
{
    public static IList<T> AsIList<T>(this IEnumerable<T> enumerable)
    {
        if (enumerable is IList<T> iList)
        {
            return iList;
        }

        return enumerable.ToList();
    }
}

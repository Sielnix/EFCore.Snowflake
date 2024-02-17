using System.Text;

namespace EFCore.Snowflake.Extensions;
internal static class StringBuilderExtensions
{
    public static StringBuilder AppendIf(this StringBuilder sb, bool condition, string value)
    {
        if (condition)
        {
            sb.Append(value);
        }

        return sb;
    }
    
    public static StringBuilder AppendJoin<T, TParam>(
        this StringBuilder stringBuilder,
        IEnumerable<T> values,
        TParam param,
        Action<StringBuilder, T, TParam> joinAction,
        string separator = ", ")
    {
        var appended = false;

        foreach (var value in values)
        {
            joinAction(stringBuilder, value, param);
            stringBuilder.Append(separator);
            appended = true;
        }

        if (appended)
        {
            stringBuilder.Length -= separator.Length;
        }

        return stringBuilder;
    }

}

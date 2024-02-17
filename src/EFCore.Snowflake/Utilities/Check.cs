using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace EFCore.Snowflake.Utilities;

[DebuggerStepThrough]
internal class Check
{
    [return: NotNull]
    public static T NotNull<T>([AllowNull][NotNull] T value, string parameterName)
    {
        if (value is null)
        {
            throw new ArgumentNullException(parameterName);
        }

        return value;
    }

    [Conditional("DEBUG")]
    public static void DebugAssert([DoesNotReturnIf(false)] bool condition, string message)
    {
        if (!condition)
        {
            throw new UnreachableException($"Check.DebugAssert failed: {message}");
        }
    }

}

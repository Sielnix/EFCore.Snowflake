using Microsoft.EntityFrameworkCore.Diagnostics;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

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

    public static string NotEmpty(
        [NotNull] string? value,
        [CallerArgumentExpression(nameof(value))] string parameterName = "")
    {
        NotNull(value, parameterName);

        if (value.AsSpan().Trim().Length == 0)
        {
            ThrowStringArgumentEmpty(parameterName);
        }

        return value;
    }

    public static string? NullButNotEmpty(
        string? value,
        [CallerArgumentExpression(nameof(value))] string parameterName = "")
    {
        if (value is not null && value.Length == 0)
        {
            ThrowStringArgumentEmpty(parameterName);
        }

        return value;
    }

    [DoesNotReturn]
    private static void ThrowStringArgumentEmpty(string parameterName)
        => throw new ArgumentException(AbstractionsStrings.ArgumentIsEmpty, parameterName);
}

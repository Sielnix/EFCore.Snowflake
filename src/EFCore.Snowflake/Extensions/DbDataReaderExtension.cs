using System.Data.Common;
using System.Diagnostics;

namespace EFCore.Snowflake.Extensions;
internal static class DbDataReaderExtension
{
    [DebuggerStepThrough]
    internal static T GetFieldValue<T>(this DbDataRecord record, string name)
    {
        return (T)record.GetValue(record.GetOrdinal(name));
    }

    [DebuggerStepThrough]
    internal static T? GetValueOrDefault<T>(this DbDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        return record.IsDBNull(ordinal)
            ? default
            : (T)record.GetValue(ordinal);
    }

    internal static long? GetValueOrDefault(this DbDataRecord record, string name)
    {
        int ordinal = record.GetOrdinal(name);
        return record.IsDBNull(ordinal)
            ? null
            : (long)record.GetValue(ordinal);
    }
}

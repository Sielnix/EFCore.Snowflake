using EFCore.Snowflake.Properties.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Logging;

namespace EFCore.Snowflake.Internal;

public static class SnowflakeLoggerExtensions
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public static void ColumnFound(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string tableName,
        string columnName,
        string dataTypeName,
        bool nullable,
        bool identity,
        string? defaultValue,
        string? computedValue)
    {
        var definition = SnowflakeResources.LogFoundColumn(diagnostics);

        if (diagnostics.ShouldLog(definition))
        {
            definition.Log(
                diagnostics,
                l => l.LogDebug(
                    definition.EventId,
                    null,
                    definition.MessageFormat,
                    tableName,
                    columnName,
                    dataTypeName,
                    nullable,
                    identity,
                    defaultValue,
                    computedValue));
        }

        // No DiagnosticsSource events because these are purely design-time messages
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public static void MissingSchemaWarning(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string? schemaName)
    {
        var definition = SnowflakeResources.LogMissingSchema(diagnostics);

        if (diagnostics.ShouldLog(definition))
        {
            definition.Log(diagnostics, schemaName);
        }

        // No DiagnosticsSource events because these are purely design-time messages
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public static void MissingTableWarning(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string? tableName)
    {
        var definition = SnowflakeResources.LogMissingTable(diagnostics);

        if (diagnostics.ShouldLog(definition))
        {
            definition.Log(diagnostics, tableName);
        }

        // No DiagnosticsSource events because these are purely design-time messages
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public static void ForeignKeyReferencesMissingPrincipalTableWarning(
        this IDiagnosticsLogger<DbLoggerCategory.Scaffolding> diagnostics,
        string? foreignKeyName,
        string? tableName,
        string? principalTableName)
    {
        var definition = SnowflakeResources.LogPrincipalTableNotInSelectionSet(diagnostics);

        if (diagnostics.ShouldLog(definition))
        {
            definition.Log(diagnostics, foreignKeyName, tableName, principalTableName);
        }

        // No DiagnosticsSource events because these are purely design-time messages
    }
}

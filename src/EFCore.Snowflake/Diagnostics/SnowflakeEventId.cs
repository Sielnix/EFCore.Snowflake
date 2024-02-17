using Microsoft.Extensions.Logging;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore.Diagnostics;

public static class SnowflakeEventId
{
    private enum Id
    {
        // Model validation events
        // Scaffolding events
        ColumnFound = CoreEventId.ProviderDesignBaseId,

        MissingSchemaWarning,
        MissingTableWarning,
        ForeignKeyReferencesMissingPrincipalTableWarning
    }

    private static readonly string ScaffoldingPrefix = DbLoggerCategory.Scaffolding.Name + ".";

    public static readonly EventId ColumnFound = MakeScaffoldingId(Id.ColumnFound);
    public static readonly EventId MissingSchemaWarning = MakeScaffoldingId(Id.MissingSchemaWarning);
    public static readonly EventId MissingTableWarning = MakeScaffoldingId(Id.MissingTableWarning);
    public static readonly EventId ForeignKeyReferencesMissingPrincipalTableWarning =
        MakeScaffoldingId(Id.ForeignKeyReferencesMissingPrincipalTableWarning);

    private static EventId MakeScaffoldingId(Id id) => new((int)id, ScaffoldingPrefix + id);
}

using Microsoft.EntityFrameworkCore.Diagnostics;

namespace EFCore.Snowflake.Diagnostics.Internal;

public class SnowflakeLoggingDefinitions : RelationalLoggingDefinitions
{
    public EventDefinitionBase? LogFoundColumn;

    public EventDefinitionBase? LogMissingSchema;

    public EventDefinitionBase? LogMissingTable;

    public EventDefinitionBase? LogPrincipalTableNotInSelectionSet;
}

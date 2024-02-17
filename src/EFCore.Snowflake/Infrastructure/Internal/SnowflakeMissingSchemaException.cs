namespace EFCore.Snowflake.Infrastructure.Internal;

internal class SnowflakeMissingSchemaException : SnowflakeEntityFrameworkException
{
    public SnowflakeMissingSchemaException(string entityName, string? entityType)
        : base(GetMessage(entityName, entityType))
    {

    }

    private static string GetMessage(string entityName, string? entityType)
    {
        return $"Entity {entityName} of type {entityType} does not have schema provided. "
            + "Either provide it in entity configuration, set default schema in DbContext.OnModelCreating or "
            + "set schema in connection string.";
    }
}

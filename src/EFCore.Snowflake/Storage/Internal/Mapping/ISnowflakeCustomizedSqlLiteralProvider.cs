namespace EFCore.Snowflake.Storage.Internal.Mapping;

public interface ISnowflakeCustomizedSqlLiteralProvider
{
    string GenerateSqlLiteralForDdl(object? value);
}

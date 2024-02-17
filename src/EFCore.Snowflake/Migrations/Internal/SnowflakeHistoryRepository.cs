using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Migrations.Internal;

public class SnowflakeHistoryRepository : HistoryRepository
{
    private RelationalTypeMapping? _stringTypeMapping;

    public SnowflakeHistoryRepository(HistoryRepositoryDependencies dependencies)
        : base(dependencies)
    {
    }

    private RelationalTypeMapping StringTypeMapping =>
        _stringTypeMapping ??= Dependencies.TypeMappingSource.GetMapping(typeof(string));

    protected override string ExistsSql
    {
        get
        {
            string query = $"""
    SELECT TO_BOOLEAN(COUNT(1)) FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA  = {StringTypeMapping.GenerateSqlLiteral(TableSchema ?? "PUBLIC")} AND TABLE_NAME = {StringTypeMapping.GenerateSqlLiteral(TableName)};
""";

            return query;
        }
    }

    public override string GetCreateScript()
    {
        string script = base.GetCreateScript();

        string query = $"""
EXECUTE IMMEDIATE
$$
BEGIN

    {script}
    
END;
$$
""";

        return query;
    }

    protected override bool InterpretExistsResult(object? value)
    {
        return (bool?)value == true;
    }

    public override string GetCreateIfNotExistsScript()
    {
        string script = GetCreateScript();

        const string createTable = "CREATE TABLE";

        return script.Insert(script.IndexOf(createTable, StringComparison.Ordinal) + createTable.Length, " IF NOT EXISTS");
    }

    public override string GetBeginIfNotExistsScript(string migrationId)
    {
        string query = $"""
EXECUTE IMMEDIATE
$$
DECLARE
	row_exists BOOLEAN;
BEGIN
	SELECT 
		TO_BOOLEAN(COUNT(1))
	INTO
		row_exists
	FROM {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)} 
		WHERE {SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName)} = {StringTypeMapping.GenerateSqlLiteral(migrationId)};
	
    IF (row_exists = false) THEN 
""";

        return query;
    }

    public override string GetBeginIfExistsScript(string migrationId)
    {
        string query = $"""
EXECUTE IMMEDIATE
$$
DECLARE
	row_exists BOOLEAN;
BEGIN
	SELECT 
		TO_BOOLEAN(COUNT(1))
	INTO
		row_exists
	FROM {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)} 
		WHERE {SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName)} = {StringTypeMapping.GenerateSqlLiteral(migrationId)};
	
    IF (row_exists) THEN 
""";

        return query;
    }

    public override string GetEndIfScript()
    {
        const string query = """
    END IF;
END;
$$
""";

        return query;
    }
}

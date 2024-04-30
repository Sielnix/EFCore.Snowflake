using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.Internal;
using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using EFCore.Snowflake.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Snowflake.Data.Client;

namespace EFCore.Snowflake.Scaffolding.Internal;

public class SnowflakeDatabaseModelFactory : DatabaseModelFactory
{
    private const string NamePartRegex = @"(?:(?:""(?<part{0}>(?:(?:"""")|[^""])+)"")|(?<part{0}>[^\.\[""]+))";

    private static readonly Regex SchemaTableNameExtractor =
        new(
            string.Format(
                CultureInfo.InvariantCulture,
                @"^{0}(?:\.{1})?$",
                string.Format(CultureInfo.InvariantCulture, NamePartRegex, 1),
                string.Format(CultureInfo.InvariantCulture, NamePartRegex, 2)),
            RegexOptions.Compiled,
            TimeSpan.FromMilliseconds(1000.0));

    private readonly ISqlGenerationHelper _sqlGenerationHelper;
    private readonly IDiagnosticsLogger<DbLoggerCategory.Scaffolding> _logger;

    public SnowflakeDatabaseModelFactory(
        ISqlGenerationHelper sqlGenerationHelper,
        IDiagnosticsLogger<DbLoggerCategory.Scaffolding> logger)
    {
        _sqlGenerationHelper = sqlGenerationHelper;
        _logger = logger;
    }

    public override DatabaseModel Create(string connectionString, DatabaseModelFactoryOptions options)
    {
        using SnowflakeDbConnection connection = new(connectionString);

        return Create(connection, options);
    }

    public override DatabaseModel Create(DbConnection connection, DatabaseModelFactoryOptions options)
    {
        DatabaseModel databaseModel = new();
        bool connectionStartedOpen = connection.State == ConnectionState.Open;
        if (!connectionStartedOpen)
        {
            connection.Open();
        }

        try
        {
            SetDatabaseDetails(databaseModel, connection);

            List<string> schemaList = options.Schemas.ToList();
            List<string> tableList = options.Tables.ToList();
            Func<string, string> schemaFilter = GenerateSchemaFilter(schemaList);
            var tablesParsed = tableList.Select(Parse).ToList();
            Func<string, string, string> tableFilter = GenerateTableFilter(schemaList, tablesParsed);

            List<DatabaseTable> tables = GetTables(connection, databaseModel.DatabaseName!, tableFilter);
            List<DatabaseSequence> sequences = GetSequences(connection, schemaFilter);

            foreach (var table in tables)
            {
                table.Database = databaseModel;
                databaseModel.Tables.Add(table);
            }

            foreach (var sequence in sequences)
            {
                sequence.Database = databaseModel;
                databaseModel.Sequences.Add(sequence);
            }

            CheckWarnings(tables, sequences, schemaList, tablesParsed);
        }
        finally
        {
            if (!connectionStartedOpen)
            {
                connection.Close();
            }
        }

        return databaseModel;
    }

    private void CheckWarnings(
        IReadOnlyCollection<DatabaseTable> tables,
        IReadOnlyCollection<DatabaseSequence> sequences,
        IReadOnlyCollection<string> schemaList,
        IReadOnlyCollection<(string? Schema, string Table)> tableList)
    {
        IEnumerable<string> foundSchemas = tables
            .Select(t => t.Schema)
            .Concat(sequences.Select(s => s.Schema))
            .Where(s => s is not null)
            .Select(s => s!);

        IEnumerable<string> notFoundSchemas = schemaList.Except(foundSchemas);

        foreach (var schema in notFoundSchemas)
        {
            _logger.MissingSchemaWarning(schema);
        }

        foreach (var (schema, table) in tableList)
        {
            if (!tables.Any(t => (!string.IsNullOrEmpty(schema) && t.Schema == schema) || t.Name == table))
            {
                _logger.MissingTableWarning(table);
            }
        }
    }

    private List<DatabaseSequence> GetSequences(
        DbConnection connection,
        Func<string, string> schemaFilter)
    {
        string query = $@"
SELECT
    SEQUENCE_NAME,
    SEQUENCE_SCHEMA,
    DATA_TYPE,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    ""INCREMENT"",
    START_VALUE,
    MINIMUM_VALUE,
    MAXIMUM_VALUE,
    CASE WHEN ORDERED = 'YES' THEN true ELSE false END AS ORDERED
FROM INFORMATION_SCHEMA.""SEQUENCES""
WHERE {schemaFilter("SEQUENCE_SCHEMA")}
";

        using var command = connection.CreateCommand();
        command.CommandText = query;
        using var reader = command.ExecuteReader();
        using var readerWrapper = ThroughDataTable(reader);

        List<DatabaseSequence> result = new();
        foreach (DbDataRecord dataRow in readerWrapper)
        {
            int? incrementBy = ToIntOrNull(dataRow.GetFieldValue<string>("INCREMENT"));
            long? startValue = ToLongOrNull(dataRow.GetFieldValue<string>("START_VALUE"));

            if (incrementBy == 1 && startValue == 1)
            {
                incrementBy = null;
                startValue = null;
            }

            long? minValue = ToLongOrNull(dataRow.GetFieldValue<string>("MINIMUM_VALUE"));
            long? maxValue = ToLongOrNull(dataRow.GetFieldValue<string>("MAXIMUM_VALUE"));

            if (minValue == long.MinValue && maxValue == long.MaxValue)
            {
                minValue = null;
                maxValue = null;
            }

            DatabaseSequence sequence = new()
            {
                Name = dataRow.GetFieldValue<string>("SEQUENCE_NAME"),
                Schema = dataRow.GetFieldValue<string>("SEQUENCE_SCHEMA"),
                IsCyclic = false,
                IncrementBy = incrementBy,
                StartValue = startValue,
                MinValue = minValue,
                MaxValue = maxValue,
                StoreType =
                    $"{dataRow.GetFieldValue<string>("DATA_TYPE")}({dataRow.GetFieldValue<long>("NUMERIC_PRECISION")},{dataRow.GetFieldValue<long>("NUMERIC_SCALE")})",
                [SnowflakeAnnotationNames.SequenceIsOrdered] = dataRow.GetFieldValue<bool>("ORDERED")
            };

            result.Add(sequence);
        }

        return result;
    }

    private int? ToIntOrNull(string numberInString)
    {
        long? longVal = ToLongOrNull(numberInString);
        if (!longVal.HasValue)
        {
            return null;
        }

        if (longVal.Value >= int.MinValue && longVal.Value <= int.MaxValue)
        {
            return (int)longVal.Value;
        }

        return null;
    }

    private long? ToLongOrNull(string numberInString)
    {
        BigInteger bigInt = BigInteger.Parse(numberInString, CultureInfo.InvariantCulture);
        BigInteger maxLong = new BigInteger(long.MaxValue);
        BigInteger minLong = new BigInteger(long.MinValue);

        if (bigInt >= minLong && bigInt <= maxLong)
        {
            return Convert.ToInt64(numberInString);
        }

        return null;
    }

    private List<DatabaseTable> GetTables(
        DbConnection connection,
        string databaseName,
        Func<string, string, string>? tableFilter)
    {
        string[] supportedTableTypes = ["BASE TABLE", "VIEW", "MATERIALIZED VIEW"];

        string tableTypeFilter = InFilter(supportedTableTypes);

        string fullTableFilter =
            tableFilter is null
            ? string.Empty
            : $"AND {tableFilter("TABLE_SCHEMA", "TABLE_NAME")}";

        string query = @$"
SELECT 
    TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, COMMENT
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    TABLE_TYPE {tableTypeFilter}
    {fullTableFilter}
";

        List<DatabaseTable> tables = new();
        using var command = connection.CreateCommand();
        command.CommandText = query;
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            string schema = reader.GetString("TABLE_SCHEMA");
            string tableName = reader.GetString("TABLE_NAME");
            string tableType = reader.GetString("TABLE_TYPE");
            string? comment = null;
            if (!reader.IsDBNull("COMMENT"))
            {
                comment = reader.GetString("COMMENT");
            }

            DatabaseTable table = tableType switch
            {
                "BASE TABLE" => new DatabaseTable(),
                "VIEW" => new DatabaseView(),
                "MATERIALIZED VIEW" => new DatabaseView(),
                _ => throw new ArgumentOutOfRangeException($"Unknown table type '{tableType}' when scaffolding {schema}.{tableName}")
            };

            table.Name = tableName;
            table.Schema = schema;
            table.Comment = comment;

            tables.Add(table);
        }

        if (tables.Any())
        {
            var tablesDict = tables.ToDictionary(t => GetTableId(t));

            GetColumns(connection, databaseName, tablesDict);
            GetPrimaryKeys(connection, databaseName, tables);
            GetUniqueKeys(connection, databaseName, tablesDict);
            GetForeignKeys(connection, databaseName, tablesDict);
        }

        return tables;
    }

    private void GetForeignKeys(
        DbConnection connection,
        string databaseName,
        IReadOnlyDictionary<TableId, DatabaseTable> tablesDict)
    {
        string query = $"SHOW IMPORTED KEYS IN DATABASE {_sqlGenerationHelper.DelimitIdentifier(databaseName)};";
        using var command = connection.CreateCommand();
        command.CommandText = query;
        using var reader = command.ExecuteReader();
        using var readerWrapper = ThroughDataTable(reader);

        var foreignKeys = readerWrapper
            .Cast<DbDataRecord>()
            .ToLookup(dr => (
                dr.GetFieldValue<string>("fk_schema_name"),
                dr.GetFieldValue<string>("fk_table_name"),
                dr.GetFieldValue<string>("fk_name")));

        if (foreignKeys.Count == 0)
        {
            return;
        }

        foreach (var foreignKeyColumns in foreignKeys)
        {
            var columns = foreignKeyColumns.AsIList();

            DbDataRecord sampleColumn = columns[0];

            // ensure that both source and destination tables are available in our tables set
            TableId fkTableId = new TableId(
                SchemaName: sampleColumn.GetFieldValue<string>("fk_schema_name"),
                TableName: sampleColumn.GetFieldValue<string>("fk_table_name"));

            TableId pkTableId = new TableId(
                SchemaName: sampleColumn.GetFieldValue<string>("pk_schema_name"),
                TableName: sampleColumn.GetFieldValue<string>("pk_table_name"));

            string foreignKeyName = sampleColumn.GetFieldValue<string>("fk_name");

            if (!tablesDict.TryGetValue(fkTableId, out DatabaseTable? table))
            {
                continue;
            }
            
            if (!tablesDict.TryGetValue(pkTableId, out DatabaseTable? principalTable)
                // ensure target table is in current database
                || sampleColumn.GetFieldValue<string>("pk_database_name") != databaseName)
            {
                _logger.ForeignKeyReferencesMissingPrincipalTableWarning(
                    foreignKeyName,
                    fkTableId.DisplayName,
                    pkTableId.DisplayName);

                continue;
            }
            
            DatabaseForeignKey foreignKey = new()
            {
                Name = foreignKeyName,
                OnDelete = GetReferentialAction(sampleColumn.GetFieldValue<string>("delete_rule")),
                Table = table,
                PrincipalTable = principalTable
            };

            // sort columns if needed
            if (columns.Count > 1)
            {
                columns = columns
                    .OrderBy(c => c.GetFieldValue<long>("key_sequence"))
                    .ToList();
            }

            foreach (var column in columns)
            {
                string columnName = column.GetFieldValue<string>("fk_column_name");
                foreignKey.Columns.Add(table.Columns.Single(c => c.Name == columnName));

                string pkColumnName = column.GetFieldValue<string>("pk_column_name");
                foreignKey.PrincipalColumns.Add(principalTable.Columns.Single(c => c.Name == pkColumnName));
            }

            table.ForeignKeys.Add(foreignKey);
        }
    }

    private ReferentialAction? GetReferentialAction(string onDeleteAction)
    {
        return onDeleteAction.ToUpperInvariant() switch
        {
            "CASCADE" => ReferentialAction.Cascade,
            "SET NULL" => ReferentialAction.SetNull,
            "SET DEFAULT" => ReferentialAction.SetDefault,
            "RESTRICT" => ReferentialAction.Restrict,
            "NO ACTION" => ReferentialAction.NoAction,
            _ => null
        };
    }

    private void GetUniqueKeys(
        DbConnection connection,
        string databaseName,
        IReadOnlyDictionary<TableId, DatabaseTable> tablesDict)
    {
        string query = $"SHOW UNIQUE KEYS IN DATABASE {_sqlGenerationHelper.DelimitIdentifier(databaseName)};";
        using var command = connection.CreateCommand();
        command.CommandText = query;
        using var reader = command.ExecuteReader();
        using var readerWrapper = ThroughDataTable(reader);

        var uniqueKeys = readerWrapper
            .Cast<DbDataRecord>()
            .ToLookup(dr => (new TableId(
                dr.GetFieldValue<string>("schema_name"),
                dr.GetFieldValue<string>("table_name")),
                    dr.GetFieldValue<string>("constraint_name")));

        if (uniqueKeys.Count == 0)
        {
            return;
        }

        foreach (var uniqueKey in uniqueKeys)
        {
            if (!tablesDict.TryGetValue(uniqueKey.Key.Item1, out DatabaseTable? table))
            {
                continue;
            }

            DatabaseUniqueConstraint uniqueConstraint = new()
            {
                Table = table,
                Name = uniqueKey.Key.Item2,
            };

            IList<DbDataRecord> columns = uniqueKey.AsIList();

            // sort columns if needed
            if (columns.Count > 1)
            {
                columns = columns
                    .OrderBy(c => c.GetFieldValue<long>("key_sequence"))
                    .ToList();
            }

            foreach (var columnRecord in columns)
            {
                string columnName = columnRecord.GetFieldValue<string>("column_name");
                uniqueConstraint.Columns.Add(table.Columns.Single(c => c.Name == columnName));
            }

            table.UniqueConstraints.Add(uniqueConstraint);
        }
    }

    private void GetPrimaryKeys(
        DbConnection connection,
        string databaseName,
        IReadOnlyList<DatabaseTable> tables)
    {
        string query = $"SHOW PRIMARY KEYS IN DATABASE {_sqlGenerationHelper.DelimitIdentifier(databaseName)};";
        using var command = connection.CreateCommand();
        command.CommandText = query;
        using var reader = command.ExecuteReader();
        using var readerWrapper = ThroughDataTable(reader);

        ILookup<TableId, DbDataRecord> colsByTable = readerWrapper
            .Cast<DbDataRecord>()
            .ToLookup(dr => new TableId(
                dr.GetFieldValue<string>("schema_name"),
                dr.GetFieldValue<string>("table_name")));

        foreach (var table in tables)
        {
            IList<DbDataRecord> pkCols = colsByTable[GetTableId(table)].AsIList();
            if (pkCols.Count == 0)
            {
                continue;
            }

            if (pkCols.Count > 1)
            {
                pkCols = pkCols
                    .OrderBy(c => c.GetFieldValue<long>("key_sequence"))
                    .ToList();
            }

            DatabasePrimaryKey primaryKey = new()
            {
                Table = table,
                Name = pkCols[0].GetFieldValue<string>("constraint_name"),
            };

            foreach (var pkCol in pkCols)
            {
                string columnName = pkCol.GetFieldValue<string>("column_name");
                primaryKey.Columns.Add(table.Columns.Single(c => c.Name == columnName));
            }

            table.PrimaryKey = primaryKey;
        }
    }

    private void GetColumns(
        DbConnection connection,
        string databaseName,
        IReadOnlyDictionary<TableId, DatabaseTable> tablesDict)
    {
        foreach (var tablePair in tablesDict)
        {
            TableId tableId = tablePair.Key;
            DatabaseTable table = tablePair.Value;

            string query = $@"
    SELECT
        C.TABLE_SCHEMA,
        C.TABLE_NAME,
        C.COLUMN_NAME,
        C.ORDINAL_POSITION,
        C.COLUMN_DEFAULT,
        CASE WHEN C.IS_NULLABLE = 'YES' THEN true ELSE false END AS IS_NULLABLE,
        C.DATA_TYPE,
        C.CHARACTER_MAXIMUM_LENGTH,
        C.NUMERIC_PRECISION,
        C.NUMERIC_SCALE,
        C.DATETIME_PRECISION,
        C.COLLATION_NAME,
        CASE WHEN C.IS_IDENTITY = 'YES' THEN true ELSE false END AS IS_IDENTITY,
        C.IDENTITY_START,
        C.IDENTITY_INCREMENT,
        C.COMMENT
    FROM INFORMATION_SCHEMA.COLUMNS C
    WHERE
        C.TABLE_SCHEMA = {EscapeLiteral(tableId.SchemaName)}
        AND C.TABLE_NAME = {EscapeLiteral(tableId.TableName)}
";

            Dictionary<ColumnId, ColumnDetailedInfo> columnsDetailedInfo = GetColumnsDetailedInfo(connection, databaseName, tableId);

            using var command = connection.CreateCommand();
            command.CommandText = query;
            using var reader = command.ExecuteReader();
            using var readerWrapper = ThroughDataTable(reader);

            List<DbDataRecord> tableCols = readerWrapper.Cast<DbDataRecord>().ToList();

            tableCols.Sort(static (x, y) =>
                x.GetFieldValue<long>("ORDINAL_POSITION").CompareTo(y.GetFieldValue<long>("ORDINAL_POSITION")));

            foreach (var columnRecord in tableCols)
            {
                string columnName = columnRecord.GetFieldValue<string>("COLUMN_NAME");
                string? columnDefault = columnRecord.GetValueOrDefault<string>("COLUMN_DEFAULT");
                bool isNullable = columnRecord.GetValueOrDefault<bool>("IS_NULLABLE");
                string dataType = columnRecord.GetFieldValue<string>("DATA_TYPE");
                long? maxLength = columnRecord.GetValueOrDefault("CHARACTER_MAXIMUM_LENGTH");
                long? numericPrecision = columnRecord.GetValueOrDefault("NUMERIC_PRECISION");
                long? numericScale = columnRecord.GetValueOrDefault("NUMERIC_SCALE");
                long? dateTimePrecision = columnRecord.GetValueOrDefault("DATETIME_PRECISION");
                string? collation = columnRecord.GetValueOrDefault<string>("COLLATION_NAME");
                bool isIdentity = columnRecord.GetFieldValue<bool>("IS_IDENTITY");
                long? identityStart = columnRecord.GetValueOrDefault("IDENTITY_START");
                long? identityIncrement = columnRecord.GetValueOrDefault("IDENTITY_INCREMENT");
                string? comment = columnRecord.GetValueOrDefault<string>("COMMENT");

                bool isSqlDefault = IsSqlDefault(columnDefault, dataType);
                ColumnDetailedInfo detailedInfo = columnsDetailedInfo[new ColumnId(tableId, columnName)];

                string storeType = GetStoreType(
                    tableId,
                    columnName,
                    dataType,
                    maxLength,
                    numericPrecision,
                    numericScale,
                    dateTimePrecision,
                    detailedInfo);

                object? defaultValue = isSqlDefault ? null : ParseClrDefault(dataType, numericPrecision, numericScale, columnDefault);
                string? defaultValueSql = columnDefault;
                string? computedColumnSql = string.IsNullOrWhiteSpace(detailedInfo.Expression) ? null : detailedInfo.Expression;

                bool? isStored = null;

                
                if (detailedInfo.IsVirtual)
                {
                    isStored = false;
                }

                DatabaseColumn column = new()
                {
                    Table = table,
                    Name = columnName,
                    Collation = collation,
                    Comment = comment,
                    ComputedColumnSql = computedColumnSql,
                    DefaultValue = defaultValue,
                    DefaultValueSql = defaultValueSql,
                    IsNullable = isNullable,
                    IsStored = isStored,
                    StoreType = storeType,
                    ValueGenerated = isIdentity ? ValueGenerated.OnAdd : null,
                };

                if (isIdentity)
                {
                    column[SnowflakeAnnotationNames.ValueGenerationStrategy] =
                        SnowflakeValueGenerationStrategy.AutoIncrement;
                    column[SnowflakeAnnotationNames.Identity] =
                        GetIdentity(tableId, columnName, identityStart, identityIncrement);
                    column[SnowflakeAnnotationNames.IdentitySeed] = identityStart;
                    column[SnowflakeAnnotationNames.IdentityIncrement] = (int)identityIncrement!.Value;
                }

                table.Columns.Add(column);

                _logger.ColumnFound(
                    tableId.DisplayName,
                    column.Name,
                    storeType,
                    isNullable,
                    isIdentity,
                    columnDefault,
                    computedColumnSql);
            }
        }
    }

    private object? ParseClrDefault(string dataTypeName, long? numericPrecision, long? numericScale, string? defaultValueSql)
    {
        if (defaultValueSql is null)
        {
            return null;
        }

        if (dataTypeName == SnowflakeStoreTypeNames.Varchar)
        {
            return defaultValueSql;
        }

        if (dataTypeName == SnowflakeStoreTypeNames.Boolean)
        {
            if (string.Equals(defaultValueSql, "TRUE", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (string.Equals(defaultValueSql, "FALSE", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            return null;
        }

        if (dataTypeName == SnowflakeStoreTypeNames.Number)
        {
            if (!numericPrecision.HasValue || !numericScale.HasValue)
            {
                throw new ArgumentException(
                    $"For '{SnowflakeStoreTypeNames.Number}' data type {nameof(numericPrecision)} and {nameof(numericScale)} must be defined");
            }

            if (numericScale.Value == 0)
            {
                SignedIntegerType safeType = SnowflakeStoreTypeNames.GetSafeIntegerType((int)numericPrecision.Value);
                switch (safeType)
                {
                    case SignedIntegerType.Byte:
                        if (byte.TryParse(defaultValueSql, CultureInfo.InvariantCulture, out byte parsedByte))
                        {
                            return parsedByte;
                        }

                        return null;
                    case SignedIntegerType.Short:
                        if (short.TryParse(defaultValueSql, CultureInfo.InvariantCulture, out short parsedShort))
                        {
                            return parsedShort;
                        }

                        return null;
                    case SignedIntegerType.Int:
                        if (int.TryParse(defaultValueSql, CultureInfo.InvariantCulture, out int parsedInt))
                        {
                            return parsedInt;
                        }

                        return null;
                    case SignedIntegerType.Long:
                        if (long.TryParse(defaultValueSql, CultureInfo.InvariantCulture, out long parsedLong))
                        {
                            return parsedLong;
                        }

                        return null;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(safeType), safeType, null);
                }
            }

            if (decimal.TryParse(defaultValueSql, CultureInfo.InvariantCulture, out decimal decimalResult))
            {
                return decimalResult;
            }
        }

        if (dataTypeName == SnowflakeStoreTypeNames.Float)
        {
            if (double.TryParse(defaultValueSql, CultureInfo.InvariantCulture, out double parsedDouble))
            {
                return parsedDouble;
            }
        }

        return null;
    }

    private Dictionary<ColumnId, ColumnDetailedInfo> GetColumnsDetailedInfo(
        DbConnection connection,
        string databaseName,
        TableId tableId)
    {
        string query = $"SHOW COLUMNS IN TABLE {_sqlGenerationHelper.DelimitIdentifier(databaseName)}.{_sqlGenerationHelper.DelimitIdentifier(tableId.TableName, tableId.SchemaName)};";

        using var command = connection.CreateCommand();
        command.CommandText = query;
        using var reader = command.ExecuteReader();
        using var readerWrapper = ThroughDataTable(reader);

        Dictionary<ColumnId, ColumnDetailedInfo> result = new();

        foreach (DbDataRecord dataRecord in readerWrapper)
        {
            string columnName = dataRecord.GetFieldValue<string>("column_name");
            string columnKind = dataRecord.GetFieldValue<string>("kind");

            bool isVirtual = columnKind switch
            {
                "COLUMN" => false,
                "VIRTUAL_COLUMN" => true,
                _ => throw new ArgumentOutOfRangeException($"Unknown column kind '{columnKind}' in column {columnName} when scaffolding {tableId.DisplayName}")
            };

            ColumnId columnId = new(
                TableId: tableId,
                ColumnName: columnName);

            string? expression = dataRecord.GetValueOrDefault<string>("expression");
            string dataTypeJson = dataRecord.GetFieldValue<string>("data_type");

            result.Add(columnId, new ColumnDetailedInfo(isVirtual, expression, dataTypeJson));
        }

        return result;
    }
    
    private DbDataReader ThroughDataTable(DbDataReader reader)
    {
        // snowflake connector doesn't implement DbDataReader enumerator
        DataTable dt = new();
        dt.Load(reader);

        return dt.CreateDataReader();
    }

    private string GetIdentity(TableId table, string columnName, long? identityStart, long? identityIncrement)
    {
        if (!identityStart.HasValue)
        {
            throw new InvalidOperationException(
                $"Invalid column {table.SchemaName}.{table.TableName}.{columnName}. " +
                $"Getting identity options but ({nameof(identityStart)} is not set");
        }

        if (!identityIncrement.HasValue)
        {
            throw new InvalidOperationException(
                $"Invalid column {table.SchemaName}.{table.TableName}.{columnName}. " +
                $"Getting identity options but ({nameof(identityIncrement)} is not set");
        }

        return FormattableString.Invariant($"START {identityStart.Value} INCREMENT {identityIncrement.Value}");
    }

    private string GetStoreType(
        TableId table,
        string columnName,
        string dataType,
        long? maxLength,
        long? numericPrecision,
        long? numericScale,
        long? dateTimePrecision,
        ColumnDetailedInfo detailedInfo)
    {
        int differentSets = 0;
        if (maxLength.HasValue)
        {
            differentSets++;
        }

        if (numericPrecision.HasValue || numericScale.HasValue)
        {
            differentSets++;
        }

        if (dateTimePrecision.HasValue)
        {
            differentSets++;
        }

        if (differentSets > 1)
        {
            throw new InvalidOperationException(
                $"Invalid column '{table.SchemaName}.{table.TableName}.{columnName}'. " +
                $"Max one of ({nameof(maxLength)}, numeric data, {nameof(dateTimePrecision)}) could be set");
        }

        if (numericPrecision.HasValue != numericScale.HasValue)
        {
            throw new InvalidOperationException(
                $"Invalid column '{table.SchemaName}.{table.TableName}.{columnName}'. " +
                $"Either both {nameof(numericPrecision)} and {nameof(numericScale)} must be set or none of them");
        }

        if (dataType == SnowflakeStoreTypeNames.Binary)
        {
            // byte length is available only through this json
            ResultDataTypeJson resultType = JsonSerializer.Deserialize<ResultDataTypeJson>(
                detailedInfo.DataTypeJson,
                new JsonSerializerOptions(JsonSerializerDefaults.Web));

            if (!resultType.Length.HasValue)
            {
                throw new InvalidOperationException(
                    $"Missing length in column '{columnName}' in table '{table.DisplayName}'");
            }

            return SnowflakeStoreTypeNames.GetBinaryType(resultType.Length.Value);
        }

        if (string.Equals(dataType, "TEXT", StringComparison.OrdinalIgnoreCase))
        {
            dataType = "VARCHAR";
        }

        if (maxLength.HasValue)
        {
            return FormattableString.Invariant($"{dataType}({maxLength.Value})");
        }

        if (numericPrecision.HasValue)
        {
            return FormattableString.Invariant($"{dataType}({numericPrecision.Value},{numericScale})");
        }

        if (dateTimePrecision.HasValue)
        {
            return FormattableString.Invariant($"{dataType}({dateTimePrecision.Value})");
        }

        return dataType;
    }

    private bool IsSqlDefault(string? dbDefaultValue, string dataType)
    {
        if (dbDefaultValue is null)
        {
            return false;
        }

        if (dataType.StartsWith("TIME", StringComparison.OrdinalIgnoreCase)
            || dataType.StartsWith("DATE", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return false;
    }

    private static (string? Schema, string Table) Parse(string table)
    {
        var match = SchemaTableNameExtractor.Match(table.Trim());

        if (!match.Success)
        {
            throw new InvalidOperationException("The table name could not be parsed.");
        }

        var part1 = match.Groups["part1"].Value;
        var part2 = match.Groups["part2"].Value;

        return string.IsNullOrEmpty(part2) ? (null, part1) : (part1, part2);
    }

    private static string InFilter(IEnumerable<string> names)
    {
        StringBuilder sb = new();
        sb.Append("IN (");
        sb.AppendJoin(", ", names.Select(s => $"'{s.Replace("'", "''")}'"));
        sb.Append(")");

        return sb.ToString();
    }

    private static Func<string, string> GenerateSchemaFilter(IReadOnlyList<string> schemas)
    {
        if (schemas.Any())
        {
            return s => $"{s} IN ({string.Join(", ", schemas.Select(EscapeLiteral))})";
        }

        return s => $"{s} <> 'INFORMATION_SCHEMA'";
    }

    /// <summary>
    ///     Builds a delegate to generate a table filter fragment.
    /// </summary>
    private static Func<string, string, string> GenerateTableFilter(
        IReadOnlyList<string> schemas,
        IReadOnlyList<(string? Schema, string Table)> tables)
        => schemas.Any() || tables.Any()
            ? (s, t) =>
            {
                var tableFilterBuilder = new StringBuilder();

                tableFilterBuilder.Append($"(({s} <> 'INFORMATION_SCHEMA') AND (").AppendLine();

                var openBracket = false;
                if (schemas.Any())
                {
                    tableFilterBuilder
                        .Append("(")
                        .Append($"{s} IN ({string.Join(", ", schemas.Select(EscapeLiteral))})");
                    openBracket = true;
                }

                if (tables.Any())
                {
                    if (openBracket)
                    {
                        tableFilterBuilder
                            .AppendLine()
                            .Append("OR ");
                    }
                    else
                    {
                        tableFilterBuilder.Append("(");
                        openBracket = true;
                    }

                    var tablesWithoutSchema = tables.Where(e => string.IsNullOrEmpty(e.Schema)).ToList();
                    if (tablesWithoutSchema.Any())
                    {
                        tableFilterBuilder.Append(t);
                        tableFilterBuilder.Append(" IN (");
                        tableFilterBuilder.Append(string.Join(", ",
                            tablesWithoutSchema.Select(e => EscapeLiteral(e.Table))));
                        tableFilterBuilder.Append(")");
                    }

                    var tablesWithSchema = tables.Where(e => !string.IsNullOrEmpty(e.Schema)).ToList();
                    if (tablesWithSchema.Any())
                    {
                        if (tablesWithoutSchema.Any())
                        {
                            tableFilterBuilder.Append(" OR ");
                        }

                        tableFilterBuilder.Append(t);
                        tableFilterBuilder.Append(" IN (");
                        tableFilterBuilder.Append(string.Join(", ",
                            tablesWithSchema.Select(e => EscapeLiteral(e.Table))));
                        tableFilterBuilder.Append(") AND (");
                        tableFilterBuilder.Append(s);
                        tableFilterBuilder.Append(" || '.' || ");
                        tableFilterBuilder.Append(t);
                        tableFilterBuilder.Append(") IN (");
                        tableFilterBuilder.Append(string.Join(", ",
                            tablesWithSchema.Select(e => EscapeLiteral($"{e.Schema}.{e.Table}"))));
                        tableFilterBuilder.Append(")");
                    }
                }

                if (openBracket)
                {
                    tableFilterBuilder.Append(")");
                }

                tableFilterBuilder.AppendLine("))");

                return tableFilterBuilder.ToString();
            }
    : (s, t) => $"{s} <> 'INFORMATION_SCHEMA'";

    private static string EscapeLiteral(string s)
        => $"'{SnowflakeStringLikeEscape.EscapeSqlLiteral(s)}'";

    private string? GetDatabaseCollation(DbConnection connection, string databaseName)
    {
        string query = $"SHOW PARAMETERS LIKE 'DEFAULT_DDL_COLLATION' IN DATABASE {_sqlGenerationHelper.DelimitIdentifier(databaseName)};";
        return GetCollationFromQuery(connection, query);
    }

    private string? GetCollationFromQuery(DbConnection connection, string query)
    {
        using DbCommand command = connection.CreateCommand();
        command.CommandText = query;

        using DbDataReader reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        object? value = reader["value"];
        if (value is null or DBNull)
        {
            return null;
        }

        string? strVal = value.ToString();
        if (string.IsNullOrEmpty(strVal))
        {
            return null;
        }

        return strVal;
    }

    private void SetDatabaseDetails(DatabaseModel databaseModel, DbConnection connection)
    {
        string connectionString = connection.ConnectionString;
        SnowflakeDbConnectionStringBuilder connectionStringBuilder = new();
        connectionStringBuilder.ConnectionString = connectionString;

        if (connectionStringBuilder.TryGetValue("db", out object? databaseName))
        {
            string? dbNameString = databaseName.ToString();
            if (!string.IsNullOrEmpty(dbNameString))
            {
                databaseModel.DatabaseName = dbNameString;
            }
        }

        if (databaseModel.DatabaseName == null)
        {
            throw new InvalidOperationException("Database is not provided in connection string");
        }

        if (connectionStringBuilder.TryGetValue("schema", out object? defaultSchema))
        {
            string? schemaNameString = defaultSchema.ToString();
            if (!string.IsNullOrEmpty(schemaNameString))
            {
                databaseModel.DefaultSchema = schemaNameString;
            }
        }

        databaseModel.Collation = GetDatabaseCollation(connection, databaseModel.DatabaseName!);
    }

    private static TableId GetTableId(DatabaseTable table)
    {
        return new TableId(
            table.Schema ?? throw new InvalidOperationException($"Table {table.Name} has missing schema"),
            table.Name);
    }

    private readonly record struct TableId(string SchemaName, string TableName)
    {
        public string DisplayName => $"{SchemaName}.{TableName}";
    };
    private readonly record struct ColumnId(TableId TableId, string ColumnName);

    private readonly record struct ColumnDetailedInfo(bool IsVirtual, string? Expression, string DataTypeJson);

    private readonly record struct ResultDataTypeJson(
        string Type,
        int? Length,
        int? ByteLength,
        int? Precision,
        int? Scale,
        bool Nullable,
        bool? Fixed);
}

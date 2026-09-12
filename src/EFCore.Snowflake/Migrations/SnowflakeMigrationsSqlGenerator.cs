using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using EFCore.Snowflake.Migrations.Operations;
using EFCore.Snowflake.Storage.Internal.Mapping;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using System.Globalization;

namespace EFCore.Snowflake.Migrations;

public class SnowflakeMigrationsSqlGenerator : MigrationsSqlGenerator
{
    private RelationalTypeMapping? _stringTypeMapping;

    public SnowflakeMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override void Generate(MigrationOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        switch (operation)
        {
            case SnowflakeCreateDatabaseOperation createDatabaseOperation:
                Generate(createDatabaseOperation, model, builder);
                break;
            case SnowflakeDropDatabaseOperation dropDatabaseOperation:
                Generate(dropDatabaseOperation, model, builder);
                break;
            case SnowflakeDropViewOperation dropViewOperation:
                Generate(dropViewOperation, model, builder);
                break;
            default:
                base.Generate(operation, model, builder);
                break;
        }
    }

    protected virtual void Generate(
        SnowflakeCreateDatabaseOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder
            .Append("CREATE DATABASE ")
            .Append(DelimitIdentifier(operation.Name));

        if (!string.IsNullOrEmpty(operation.Collation))
        {
            builder
                .AppendLine()
                .Append("DEFAULT_DDL_COLLATION ")
                .Append(GenerateSqlLiteral(operation.Collation));
        }

        builder
            .Append(StatementTerminator)
            .AppendLine()
            .EndCommand(suppressTransaction: true);
    }

    protected virtual void Generate(
        SnowflakeDropDatabaseOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder
            .Append("DROP DATABASE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator)
            .EndCommand(suppressTransaction: true);
    }

    protected virtual void Generate(
        SnowflakeDropViewOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder
            .Append("DROP VIEW ")
            .Append(DelimitIdentifier(operation.Name, operation.Schema))
            .AppendLine(StatementTerminator)
            .EndCommand(suppressTransaction: true);
    }

    protected override void Generate(
        EnsureSchemaOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)

    {
        builder
            .Append("CREATE SCHEMA IF NOT EXISTS ")
            .Append(DelimitIdentifier(operation.Name))
            .AppendLine(StatementTerminator)
            .EndCommand();
    }

    protected override void Generate(
        AlterColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        if (operation.ComputedColumnSql != null && operation.IsStored == true)
        {
            ThrowNoStoredCalculatedColumns();
        }

        if (operation.Collation != operation.OldColumn.Collation)
        {
            throw new NotSupportedException(
                "Collation change is not supported in Snowflake. Column has to be dropped and recreated with different collation");
        }

        bool oldHasIdentity = IsIdentity(operation.OldColumn);
        bool newHasIdentity = IsIdentity(operation);
        if (newHasIdentity && !oldHasIdentity)
        {
            throw new InvalidOperationException("To add AutoIncrement property to a column, the column needs to be dropped and recreated.");
        }

        if (oldHasIdentity && !newHasIdentity)
        {
            builder
                .Append("ALTER TABLE ")
                .Append(DelimitIdentifier(operation.Table, operation.Schema))
                .Append(" ALTER COLUMN ")
                .Append(DelimitIdentifier(operation.Name))
                .Append(" DROP DEFAULT")
                .AppendLine(StatementTerminator)
                .EndCommand();
        }

        if (operation.ComputedColumnSql != operation.OldColumn.ComputedColumnSql)
        {
            // drop and recreate column if it is computed
            Generate(
                new DropColumnOperation
                {
                    Schema = operation.Schema,
                    Table = operation.Table,
                    Name = operation.Name
                },
                model,
                builder);

            var addColumnOperation = new AddColumnOperation
            {
                Schema = operation.Schema,
                Table = operation.Table,
                Name = operation.Name,
                ClrType = operation.ClrType,
                ColumnType = operation.ColumnType,
                IsUnicode = operation.IsUnicode,
                IsFixedLength = operation.IsFixedLength,
                MaxLength = operation.MaxLength,
                Precision = operation.Precision,
                Scale = operation.Scale,
                IsRowVersion = operation.IsRowVersion,
                IsNullable = operation.IsNullable,
                DefaultValue = operation.DefaultValue,
                DefaultValueSql = operation.DefaultValueSql,
                ComputedColumnSql = operation.ComputedColumnSql,
                IsStored = operation.IsStored,
                Comment = operation.Comment,
                Collation = operation.Collation
            };
            addColumnOperation.AddAnnotations(operation.GetAnnotations());

            Generate(addColumnOperation, model, builder);

            return;
        }

        string? type = operation.ColumnType ?? GetColumnType(operation.Schema, operation.Table, operation.Name, operation, model);

        if (operation.IsNullable != operation.OldColumn.IsNullable)
        {
            if (!operation.IsNullable && (operation.DefaultValueSql is not null || operation.DefaultValue is not null))
            {
                string defaultValueSql;
                if (operation.DefaultValueSql is not null)
                {
                    defaultValueSql = operation.DefaultValueSql;
                }
                else
                {
                    Check.DebugAssert(operation.DefaultValue is not null, "operation.DefaultValue is not null");

                    RelationalTypeMapping? typeMapping = null;
                    if (type != null)
                    {
                        typeMapping = Dependencies.TypeMappingSource.FindMapping(operation.DefaultValue.GetType(), type);
                    }

                    typeMapping ??= Dependencies.TypeMappingSource.GetMappingForValue(operation.DefaultValue);

                    defaultValueSql = typeMapping.GenerateSqlLiteral(operation.DefaultValue);
                }

                builder
                    .Append("UPDATE ")
                    .Append(DelimitIdentifier(operation.Table, operation.Schema))
                    .Append(" SET ")
                    .Append(DelimitIdentifier(operation.Name))
                    .Append(" = ")
                    .Append(defaultValueSql)
                    .Append(" WHERE ")
                    .Append(DelimitIdentifier(operation.Name))
                    .Append(" IS NULL")
                    .AppendLine(StatementTerminator)
                    .EndCommand();
            }

            builder
                .Append("ALTER TABLE ")
                .Append(DelimitIdentifier(operation.Table, operation.Schema))
                .Append(" ALTER COLUMN ")
                .Append(DelimitIdentifier(operation.Name))
                .Append(operation.IsNullable ? " DROP NOT NULL" : " SET NOT NULL")
                .AppendLine(StatementTerminator)
                .EndCommand();
        }

        if (type != operation.OldColumn.ColumnType)
        {
            if (type == null)
            {
                throw new NotSupportedException(
                    "Column type is required if it's not calculated (Virtual) column");
            }

            builder
                .Append("ALTER TABLE ")
                .Append(DelimitIdentifier(operation.Table, operation.Schema))
                .Append(" ALTER COLUMN ")
                .Append(DelimitIdentifier(operation.Name))
                .Append(" SET DATA TYPE ")
                .Append(type)
                .AppendLine(StatementTerminator)
                .EndCommand();
        }

        if (operation.Comment != operation.OldColumn.Comment)
        {
            builder
                .Append("ALTER TABLE ")
                .Append(DelimitIdentifier(operation.Table, operation.Schema))
                .Append(" ALTER COLUMN ")
                .Append(DelimitIdentifier(operation.Name));

            if (operation.Comment == null)
            {
                builder.Append(" UNSET COMMENT");
            }
            else
            {
                builder
                    .Append(" COMMENT ")
                    .Append(GenerateSqlLiteral(operation.Comment));
            }

            builder
                .AppendLine(StatementTerminator)
                .EndCommand();
        }
    }

    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        SnowflakeTableType tableType = GetTableType(operation);

        string tableTypeQuery = tableType switch
        {
            SnowflakeTableType.Permanent => string.Empty,
            SnowflakeTableType.Transient => " TRANSIENT",
            SnowflakeTableType.Hybrid => " HYBRID",
            _ => throw new ArgumentOutOfRangeException(nameof(tableType), tableType, null)
        };

        builder
            .Append("CREATE")
            .Append(tableTypeQuery)
            .Append(" TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema))
            .AppendLine(" (");

        using (builder.Indent())
        {
            CreateTableColumns(operation, model, builder);
            CreateTableConstraints(operation, model, builder);
            builder.AppendLine();
        }

        builder.Append(")");
        
        if (operation.Comment != null)
        {
            builder
                .Append(" COMMENT = ")
                .Append(GenerateSqlLiteral(operation.Comment));
        }

        if (terminate)
        {
            builder
                .AppendLine(StatementTerminator)
                .EndCommand();
        }
    }

    protected override void Generate(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder
            .Append("ALTER TABLE ")
            .Append(DelimitIdentifier(operation.Name, operation.Schema))
            .Append(" RENAME TO ")
            .Append(DelimitIdentifier(operation.NewName ?? operation.Name, operation.NewSchema))
            .AppendLine(StatementTerminator)
            .EndCommand();
    }

    protected override void Generate(CreateSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        // Snowflake sequences don't have a type; specifying long to avoid AS clause
        operation.ClrType = typeof(long);
        base.Generate(operation, model, builder);
    }

    protected override void SequenceOptions(
        string? schema,
        string name,
        SequenceOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool forAlter)
    {
        bool isOrdered = (bool)operation.GetAnnotation(SnowflakeAnnotationNames.SequenceIsOrdered).Value!;

        builder
            .Append(" INCREMENT BY ").Append(operation.IncrementBy.ToString(CultureInfo.InvariantCulture))
            .Append(" ").Append(isOrdered ? "ORDER" : "NOORDER");
    }

    protected override void Generate(RenameSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder
            .Append("ALTER SEQUENCE ")
            .Append(DelimitIdentifier(operation.Name, operation.Schema))
            .Append(" RENAME TO ")
            .Append(DelimitIdentifier(operation.NewName ?? operation.Name, operation.NewSchema))
            .AppendLine(StatementTerminator)
            .EndCommand();
    }

    protected override void Generate(AlterSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (operation.IncrementBy != operation.OldSequence.IncrementBy)
        {
            builder
                .Append("ALTER SEQUENCE ")
                .Append(DelimitIdentifier(operation.Name, operation.Schema))
                .Append(" SET INCREMENT BY ")
                .Append(operation.IncrementBy.ToString(CultureInfo.InvariantCulture))
                .AppendLine(StatementTerminator)
                .EndCommand();
        }

        bool oldIsOrdered = (bool)operation.OldSequence.GetAnnotation(SnowflakeAnnotationNames.SequenceIsOrdered).Value!;
        bool newIsOrdered = (bool)operation.GetAnnotation(SnowflakeAnnotationNames.SequenceIsOrdered).Value!;

        if (oldIsOrdered != newIsOrdered)
        {
            builder
                .Append("ALTER SEQUENCE ")
                .Append(DelimitIdentifier(operation.Name, operation.Schema))
                .Append(" SET ").Append(newIsOrdered ? "ORDER" : "NOORDER")
                .AppendLine(StatementTerminator)
                .EndCommand();
        }
    }

    protected override void Generate(RestartSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        throw new NotSupportedException("Sequence restarting is not supported in Snowflake");
    }

    protected override void Generate(AlterTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (GetTableType(operation) != GetTableType(operation.OldTable))
        {
            throw new InvalidOperationException(
                "To change the table type, the table needs to be dropped and recreated.");
        }

        if (operation.Comment != operation.OldTable.Comment)
        {
            builder
                .Append("ALTER TABLE ")
                .Append(DelimitIdentifier(operation.Name, operation.Schema));

            if (operation.Comment == null)
            {
                builder.Append(" UNSET COMMENT");
            }
            else
            {
                builder
                    .Append(" SET COMMENT = ")
                    .Append(GenerateSqlLiteral(operation.Comment));
            }

            builder
                .AppendLine(StatementTerminator)
                .EndCommand();
        }

        base.Generate(operation, model, builder);
    }

    protected override void Generate(DropCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        ThrowNoChecks();
    }

    protected override void Generate(AddCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        ThrowNoChecks();
    }

    protected override void CheckConstraint(AddCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        ThrowNoChecks();
    }

    private static void ThrowNoChecks()
    {
        throw new NotSupportedException("Snowflake does not support check constraints");
    }

    private static void ThrowNoStoredCalculatedColumns()
    {
        throw new NotSupportedException(
            "Stored generated columns are not supported by Snowflake, specify 'stored: false' in "
            + $"'{nameof(RelationalPropertyBuilderExtensions.HasComputedColumnSql)}' in your context's OnModelCreating.");
    }

    protected override void Generate(
        CreateIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        CheckIndexHandling(model);
    }

    protected override void Generate(
        RenameIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        CheckIndexHandling(model);
    }

    protected override void Generate(
        DropIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        CheckIndexHandling(model);
    }

    protected override void ComputedColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        var columnType = operation.ColumnType ?? GetColumnType(schema, table, name, operation, model)!;
        builder
            .Append(DelimitIdentifier(name))
            .Append(" ")
            .Append(columnType);

        if (operation.Collation != null)
        {
            builder
                .Append(" COLLATE ")
                .Append(GenerateSqlLiteral(operation.Collation));
        }

        if (operation.IsStored.HasValue && operation.IsStored.Value)
        {
            ThrowNoStoredCalculatedColumns();
        }

        builder.Append(" AS ");
        if (operation.Collation == null)
        {
            builder
                .Append("(")
                .Append(operation.ComputedColumnSql!)
                .Append(")");
        }
        else
        {
            builder
                .Append("COLLATE(")
                .Append(operation.ComputedColumnSql!)
                .Append(", ")
                .Append(GenerateSqlLiteral(operation.Collation))
                .Append(")");
        }
    }

    protected override void ColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        if (operation.ComputedColumnSql != null)
        {
            ComputedColumnDefinition(schema, table, name, operation, model, builder);
        }
        else
        {
            var columnType = operation.ColumnType ?? GetColumnType(schema, table, name, operation, model)!;
            builder
                .Append(DelimitIdentifier(name))
                .Append(" ")
                .Append(columnType);

            if (operation.Collation != null)
            {
                builder
                    .Append(" COLLATE ")
                    .Append(GenerateSqlLiteral(operation.Collation));
            }

            builder.Append(operation.IsNullable ? " NULL" : " NOT NULL");

            DefaultValue(operation.DefaultValue, operation.DefaultValueSql, columnType, builder);

            string? identity = operation[SnowflakeAnnotationNames.Identity] as string;
            if (identity != null
                || operation[SnowflakeAnnotationNames.ValueGenerationStrategy] as SnowflakeValueGenerationStrategy?
                == SnowflakeValueGenerationStrategy.AutoIncrement)
            {
                builder.Append(" AUTOINCREMENT");
                if (string.IsNullOrEmpty(identity))
                {
                    builder.Append(" START 1 INCREMENT 1 ORDER");
                }
                else
                {
                    builder
                        .Append(" ")
                        .Append(identity);

                    if (!identity.Contains("ORDER", StringComparison.OrdinalIgnoreCase))
                    {
                        // backward compatibility. Add order if it's not set
                        builder.Append(" ORDER");
                    }
                }
            }
        }

        if (operation.Comment != null)
        {
            builder
                .Append(" COMMENT ")
                .Append(GenerateSqlLiteral(operation.Comment));
        }
    }

    protected override void Generate(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder
            .Append("ALTER TABLE ")
            .Append(DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" RENAME COLUMN ")
            .Append(DelimitIdentifier(operation.Name))
            .Append(" TO ")
            .Append(DelimitIdentifier(operation.NewName))
            .AppendLine(StatementTerminator)
            .EndCommand();
    }

    protected override void Generate(
        InsertDataOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        var rowCount = operation.Values.GetLength(0);
        var columnCount = operation.Values.GetLength(1);
        for (var currentRow = 0; currentRow < rowCount; currentRow++)
        {
            var rowValues = new object?[1, columnCount];
            Array.Copy(operation.Values, currentRow * columnCount, rowValues, 0, columnCount);

            var insertRowOperation = new InsertDataOperation
            {
                Schema = operation.Schema,
                Table = operation.Table,
                Columns = operation.Columns,
                ColumnTypes = operation.ColumnTypes,
                Values = rowValues
            };
            insertRowOperation.AddAnnotations(operation.GetAnnotations());

            // Snowflake.Data only supports one statement per command; generating one command per row
            base.Generate(insertRowOperation, model, builder, terminate || currentRow != rowCount - 1);
        }
    }

    protected override void DefaultValue(
        object? defaultValue,
        string? defaultValueSql,
        string? columnType,
        MigrationCommandListBuilder builder)
    {
        if (defaultValue != null && defaultValueSql == null)
        {
            var typeMapping = (columnType != null
                                  ? Dependencies.TypeMappingSource.FindMapping(defaultValue.GetType(), columnType)
                                  : null)
                              ?? Dependencies.TypeMappingSource.GetMappingForValue(defaultValue);
            if (typeMapping is ISnowflakeCustomizedSqlLiteralProvider snowflakeCustomized)
            {
                builder
                    .Append(" DEFAULT ")
                    .Append(snowflakeCustomized.GenerateSqlLiteralForDdl(defaultValue));

                return;
            }
        }

        base.DefaultValue(defaultValue, defaultValueSql, columnType, builder);
    }

    private string DelimitIdentifier(string identifier)
        => Dependencies.SqlGenerationHelper.DelimitIdentifier(identifier);

    private string DelimitIdentifier(string name, string? schema)
        => Dependencies.SqlGenerationHelper.DelimitIdentifier(name, schema);

    private string GenerateSqlLiteral(string value)
        => (_stringTypeMapping ??= Dependencies.TypeMappingSource.GetMapping(typeof(string))).GenerateSqlLiteral(value);

    private string StatementTerminator => Dependencies.SqlGenerationHelper.StatementTerminator;

    private void CheckIndexHandling(IModel? model)
    {
        SnowflakeIndexBehavior? indexBehavior = model?.GetIndexBehavior();

        if (indexBehavior is null or SnowflakeIndexBehavior.Ignore)
        {
            return;
        }

        throw new NotSupportedException(
            $"Index behavior is set to {indexBehavior}, any index operations are blocked since Snowflake doesn't support indexes. " +
            $"If you want to ignore all index definitions set call HasIndexBehavior(SnowflakeIndexBehavior.Ignore) on model");
    }

    private static bool IsIdentity(ColumnOperation operation)
        => operation[SnowflakeAnnotationNames.Identity] != null
           || operation[SnowflakeAnnotationNames.ValueGenerationStrategy] as SnowflakeValueGenerationStrategy?
           == SnowflakeValueGenerationStrategy.AutoIncrement;

    private static SnowflakeTableType GetTableType(TableOperation operation)
    {
        object? tableType = operation[SnowflakeAnnotationNames.TableType];
        if (tableType is null)
        {
            // this should not be here, annotation always should be set
            // probably bug in efcore, to be removed after fixing https://github.com/dotnet/efcore/issues/34032
            return SnowflakeTableType.Permanent;
        }

        return (SnowflakeTableType)tableType;
    }
}

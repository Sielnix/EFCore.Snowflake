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
using System.Text;

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
            .Append(StatementTerminator)
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
                .Append("DROP DEFAULT")
                .Append(StatementTerminator)
                .EndCommand();
        }

        if (operation.ComputedColumnSql != operation.OldColumn.ComputedColumnSql)
        {
            // drop and recreate column if it is computed
            builder
                .Append("ALTER TABLE ")
                .Append(DelimitIdentifier(operation.Table, operation.Schema))
                .Append(" DROP COLUMN ")
                .Append(DelimitIdentifier(operation.Name))
                .Append(StatementTerminator)
                .EndCommand();

            builder
                .Append("ALTER TABLE ")
                .Append(DelimitIdentifier(operation.Table, operation.Schema))
                .Append(" ADD ");

            ColumnDefinition(
                operation.Schema,
                operation.Table,
                operation.Name,
                operation,
                model,
                builder);

            builder
                .Append(StatementTerminator)
                .EndCommand();

            return;
        }
        
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

                    string? type = operation.ColumnType ?? GetColumnType(operation.Schema, operation.Table, operation.Name, operation, model);

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
                .Append(StatementTerminator)
                .EndCommand();
        }

        if (operation.ColumnType != operation.OldColumn.ColumnType)
        {
            if (operation.ColumnType == null)
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
                .Append(operation.ColumnType)
                .Append(StatementTerminator)
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
                .Append(StatementTerminator)
                .EndCommand();
        }
    }

    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        base.Generate(operation, model, builder, terminate: false);

        if (operation.Comment != null)
        {
            builder
                .AppendLine()
                .Append(" COMMENT = ")
                .Append(GenerateSqlLiteral(operation.Comment));
        }

        if (terminate)
        {
            builder
                .Append(StatementTerminator)
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
        bool isOrdered = (bool)operation.GetAnnotation(SnowflakeAnnotationNames.SequenceIsOrdered).Value!;

        builder
            .Append("CREATE SEQUENCE ")
            .Append(DelimitIdentifier(operation.Name, operation.Schema)).AppendLine()
            .Append("START WITH ").Append(operation.StartValue.ToString(CultureInfo.InvariantCulture))
            .Append(" INCREMENT BY ").Append(operation.IncrementBy.ToString(CultureInfo.InvariantCulture))
            .Append(" ").Append(isOrdered ? "ORDER" : "NOORDER")
            .AppendLine(StatementTerminator)
            .EndCommand();
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

    protected override void ColumnDefinition(
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

        if (operation.ComputedColumnSql != null)
        {
            if (operation.IsStored.HasValue && operation.IsStored.Value)
            {
                ThrowNoStoredCalculatedColumns();
            }

            builder.Append(" AS ");
            if (operation.Collation == null)
            {
                builder
                    .Append("(")
                    .Append(operation.ComputedColumnSql)
                    .Append(")");
            }
            else
            {
                builder
                    .Append("COLLATE(")
                    .Append(operation.ComputedColumnSql)
                    .Append(", ")
                    .Append(GenerateSqlLiteral(operation.Collation))
                    .Append(")");
            }

            return;
        }

        builder.Append(operation.IsNullable ? " NULL" : " NOT NULL");

        DefaultValue(operation.DefaultValue, operation.DefaultValueSql, columnType, builder);

        string? identity = operation[SnowflakeAnnotationNames.Identity] as string;
        if (identity != null
            || operation[SnowflakeAnnotationNames.ValueGenerationStrategy] as SnowflakeValueGenerationStrategy?
            == SnowflakeValueGenerationStrategy.AutoIncrement)
        {
            builder.Append(" AUTOINCREMENT ");
            if (string.IsNullOrEmpty(identity))
            {
                builder.Append(" START 1 INCREMENT 1 ORDER");
            }
            else
            {
                builder.Append(identity);

                if (!identity.Contains("ORDER", StringComparison.OrdinalIgnoreCase))
                {
                    // backward compatibility. Add order if it's not set
                    builder.Append(" ORDER");
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

    protected override void ForeignKeyConstraint(AddForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        base.ForeignKeyConstraint(operation, model, builder);
    }

    protected override void Generate(
        InsertDataOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        var sqlBuilder = new StringBuilder();
        foreach (var modificationCommand in GenerateModificationCommands(operation, model))
        {
            SqlGenerator.AppendInsertOperation(
                sqlBuilder,
                modificationCommand,
                0);

            builder
                .Append(sqlBuilder.ToString())
                .EndCommand();

            sqlBuilder.Clear();
        }
    }

    protected override void DefaultValue(
        object? defaultValue,
        string? defaultValueSql,
        string? columnType,
        MigrationCommandListBuilder builder)
    {
        if (defaultValueSql != null)
        {
            builder
                .Append(" DEFAULT (")
                .Append(defaultValueSql)
                .Append(")");
        }
        else if (defaultValue != null)
        {
            var typeMapping = (columnType != null
                                  ? Dependencies.TypeMappingSource.FindMapping(defaultValue.GetType(), columnType)
                                  : null)
                              ?? Dependencies.TypeMappingSource.GetMappingForValue(defaultValue);

            string sqlLiteral;
            if (typeMapping is ISnowflakeCustomizedSqlLiteralProvider snowflakeCustomized)
            {
                sqlLiteral = snowflakeCustomized.GenerateSqlLiteralForDdl(defaultValue);
            }
            else
            {
                sqlLiteral = typeMapping.GenerateSqlLiteral(defaultValue);
            }

            builder
                .Append(" DEFAULT ")
                .Append(sqlLiteral);
        }
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
}

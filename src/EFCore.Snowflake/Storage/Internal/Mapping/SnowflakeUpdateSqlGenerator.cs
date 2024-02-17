using System.Data;
using System.Text;
using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Array = System.Array;

namespace EFCore.Snowflake.Storage.Internal.Mapping;

public class SnowflakeUpdateSqlGenerator : UpdateAndSelectSqlGenerator
{
    public SnowflakeUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
        SqlGenerationHelper = (SnowflakeSqlGenerationHelper)dependencies.SqlGenerationHelper;
    }

    protected new virtual SnowflakeSqlGenerationHelper SqlGenerationHelper { get; }

    public virtual ResultSetMapping AppendPostModificationSelectOperation(
        StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition,
        out bool requiresTransaction)
    {
        IReadOnlyList<IColumnModification> operations = command.ColumnModifications;

        requiresTransaction = true;

        return AppendSelectAffectedCommand(
            commandStringBuilder,
            command.TableName,
            command.Schema,
            operations,
            commandPosition);
    }

    protected override ResultSetMapping AppendInsertAndSelectOperation(StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command, int commandPosition, out bool requiresTransaction)
    {
        var name = command.TableName;
        var schema = command.Schema;
        var operations = command.ColumnModifications;

        
        //var readOperations = operations.Where(o => o.IsRead).ToList();

        AppendInsertCommand(commandStringBuilder, name, schema, operations);

        
        if (operations.Any(o => o.IsRead))
        {
            // WE don't do actual select since Snowflake doesn't support two commands in one batch
            // Instead we just do the regular insert and do the select as command batch, which means separate DbCommand
            requiresTransaction = true;
        }
        else
        {
            requiresTransaction = false;
        }
        

        return AppendSelectAffectedCountCommand(commandStringBuilder, name, schema, commandPosition);
    }

    protected virtual void AppendInsertCommand(
        StringBuilder commandStringBuilder,
        string name,
        string? schema,
        IReadOnlyList<IColumnModification> operations)
    {
        var writeOperations = operations.Where(o => o.IsWrite).ToArray();
        bool useSelect = writeOperations.Any();

        var forCommandHeaderOperations = writeOperations;
        if (!useSelect)
        {
            forCommandHeaderOperations = [operations.First(o => o.Entry != null && o.Property!= null && o.Entry.IsStoreGenerated(o.Property))];
        }

        AppendInsertCommandHeader(commandStringBuilder, name, schema, forCommandHeaderOperations);
        
        commandStringBuilder.AppendLine();
        commandStringBuilder.Append(useSelect ? "SELECT " : "VALUES ");


        AppendValues(
            commandStringBuilder,
            name,
            schema,
            wrapInBracket: !useSelect,
            useSelect ? writeOperations : forCommandHeaderOperations);

        commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);
    }

    protected override void AppendValues(StringBuilder commandStringBuilder, string name, string? schema,
        IReadOnlyList<IColumnModification> operations)
    {
        if (operations.Count > 0)
        {
            commandStringBuilder
                .Append('(')
                .AppendJoin(
                    operations,
                    (this, name, schema),
                    (sb, o, p) =>
                    {
                        if (o.IsWrite)
                        {
                            var (g, n, s) = p;
                            if (!o.UseCurrentValueParameter)
                            {
                                AppendSqlLiteral(sb, o, n, s);
                            }
                            else
                            {
                                g.SqlGenerationHelper.GenerateParameterNamePlaceholder(sb, o.ParameterName,
                                    o.TypeMapping);
                            }
                        }
                        else
                        {
                            sb.Append("DEFAULT");
                        }
                    })
                .Append(')');
        }
    }

    protected virtual void AppendValues(
        StringBuilder commandStringBuilder,
        string name,
        string? schema,
        bool wrapInBracket,
        IReadOnlyList<IColumnModification> operations)
    {
        commandStringBuilder
            .AppendIf(wrapInBracket, "(")
            .AppendJoin(
                operations,
                (this, name, schema),
                (sb, o, p) =>
                {
                    if (o.IsWrite)
                    {
                        var (g, n, s) = p;
                        if (!o.UseCurrentValueParameter)
                        {
                            AppendSqlLiteral(sb, o, n, s);
                        }
                        else
                        {
                            g.SqlGenerationHelper.GenerateParameterNamePlaceholder(sb, o.ParameterName, o.TypeMapping);
                        }
                    }
                    else
                    {
                        sb.Append("DEFAULT");
                    }
                })
            .AppendIf(wrapInBracket, ")");
    }

    protected override ResultSetMapping AppendUpdateAndSelectOperation(
        StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition,
        out bool requiresTransaction)
    {
        var name = command.TableName;
        var schema = command.Schema;
        var operations = command.ColumnModifications;

        var writeOperations = operations.Where(o => o.IsWrite).ToList();
        var conditionOperations = operations.Where(o => o.IsCondition).ToList();
        var readOperations = operations.Where(o => o.IsRead).ToList();

        AppendUpdateCommand(commandStringBuilder, name, schema, writeOperations, Array.Empty<IColumnModification>(), conditionOperations);

        if (readOperations.Count > 0)
        {
            // WE don't do actual select since Snowflake doesn't support two commands in one batch
            // Instead we just do the regular insert and do the select as command batch, which means separate DbCommand
            requiresTransaction = true;

            return AppendSelectAffectedCountCommand(commandStringBuilder, name, schema, commandPosition);
        }

        requiresTransaction = false;

        return AppendSelectAffectedCountCommand(commandStringBuilder, name, schema, commandPosition);
    }

    protected virtual ResultSetMapping AppendSelectAffectedCommand(
        StringBuilder commandStringBuilder,
        string name,
        string? schema,
        IReadOnlyList<IColumnModification> allColumns,
        int commandPosition)
    {
        List<IColumnModification> readOperations = allColumns.Where(o => o.IsRead).ToList();

        if (!readOperations.Any())
        {
            throw new InvalidOperationException("Post modification select shouldn't be called if there are zero reads");
        }

        commandStringBuilder.Append("SELECT ");
        bool isFirst = true;

        foreach (var readOperation in readOperations.Where(c => c.IsRead))
        {
            if (isFirst)
            {
                isFirst = false;
            }
            else
            {
                commandStringBuilder.Append(", ");
            }

            if (IsIdentityOperation(readOperation))
            {
                commandStringBuilder
                    .Append("MAX(")
                    .Append(SqlGenerationHelper.DelimitIdentifier(readOperation.ColumnName))
                    .Append(") AS ")
                    .Append(SqlGenerationHelper.DelimitIdentifier(readOperation.ColumnName));
            }
        }

        AppendFromClause(commandStringBuilder, name, schema);
        commandStringBuilder.Append(" AT(statement=>last_query_id())");

        isFirst = true;
        foreach (var columnModification in allColumns)
        {
            if (IsIdentityOperation(columnModification) || columnModification.IsRead)
            {
                continue;
            }

            if (isFirst)
            {
                commandStringBuilder
                    .AppendLine()
                    .Append("WHERE ");
                isFirst = false;
            }
            else
            {
                commandStringBuilder.Append(" AND ");
            }

            AppendWhereCondition(commandStringBuilder, columnModification, useOriginalValue: false);
        }

        commandStringBuilder
            .AppendLine(SqlGenerationHelper.StatementTerminator)
            .AppendLine();

        return ResultSetMapping.LastInResultSet;
    }

    protected override void AppendIdentityWhereCondition(StringBuilder commandStringBuilder, IColumnModification columnModification)
    {
        commandStringBuilder.Append("1 = 1");
    }

    protected override void AppendRowsAffectedWhereCondition(StringBuilder commandStringBuilder, int expectedRowsAffected)
    {
        commandStringBuilder.Append("1 = 1");
    }

    protected override void AppendUpdateColumnValue(
        ISqlGenerationHelper updateSqlGeneratorHelper,
        IColumnModification columnModification,
        StringBuilder stringBuilder,
        string name,
        string? schema)
    {
        if (!columnModification.UseCurrentValueParameter)
        {
            AppendSqlLiteral(stringBuilder, columnModification, name, schema);
        }
        else
        {
            SqlGenerationHelper.GenerateParameterNamePlaceholder(
                stringBuilder, columnModification.ParameterName, columnModification.TypeMapping);
        }
    }

    public override ResultSetMapping AppendStoredProcedureCall(
        StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition,
        out bool requiresTransaction)
    {
        // COPY&PASTE FROM base method
        Check.DebugAssert(command.StoreStoredProcedure is not null, "command.StoredProcedure is not null");

        var storedProcedure = command.StoreStoredProcedure;

        var resultSetMapping = ResultSetMapping.NoResults;

        foreach (var resultColumn in storedProcedure.ResultColumns)
        {
            resultSetMapping = ResultSetMapping.LastInResultSet;

            if (resultColumn == command.RowsAffectedColumn)
            {
                resultSetMapping |= ResultSetMapping.ResultSetWithRowsAffectedOnly;
            }
            else
            {
                resultSetMapping = ResultSetMapping.LastInResultSet;
                break;
            }
        }

        Check.DebugAssert(
            storedProcedure.Parameters.Any() || storedProcedure.ResultColumns.Any(),
            "Stored procedure call with neither parameters nor result columns");

        commandStringBuilder.Append("CALL ");

        SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, storedProcedure.Name, storedProcedure.Schema);

        commandStringBuilder.Append('(');

        var first = true;

        // Only positional parameter style supported for now, see #28439

        // Note: the column modifications are already ordered according to the sproc parameter ordering
        // (see ModificationCommand.GenerateColumnModifications)
        for (var i = 0; i < command.ColumnModifications.Count; i++)
        {
            var columnModification = command.ColumnModifications[i];

            if (columnModification.Column is not IStoreStoredProcedureParameter parameter)
            {
                continue;
            }

            if (first)
            {
                first = false;
            }
            else
            {
                commandStringBuilder.Append(", ");
            }

            Check.DebugAssert(columnModification.UseParameter, "Column modification matched a parameter, but UseParameter is false");

            SqlGenerationHelper.GenerateParameterNamePlaceholder(
                commandStringBuilder, columnModification.UseOriginalValueParameter
                    ? columnModification.OriginalParameterName!
                    : columnModification.ParameterName!,
                // ONLY CHANGE
                typeMapping: columnModification.TypeMapping);

            if (parameter.Direction.HasFlag(ParameterDirection.Output))
            {
                resultSetMapping |= ResultSetMapping.HasOutputParameters;
            }
        }

        commandStringBuilder.Append(')');

        commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);

        requiresTransaction = true;

        return resultSetMapping;
    }

    protected override void AppendWhereCondition(
        StringBuilder commandStringBuilder,
        IColumnModification columnModification,
        bool useOriginalValue)
    {
        // COPY&PASTE from base method
        SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, columnModification.ColumnName);

        var parameterValue = useOriginalValue
            ? columnModification.OriginalValue
            : columnModification.Value;

        if (parameterValue == null)
        {
            commandStringBuilder.Append(" IS NULL");
        }
        else
        {
            commandStringBuilder.Append(" = ");
            if (!columnModification.UseParameter)
            {
                AppendSqlLiteral(commandStringBuilder, columnModification, null, null);
            }
            else
            {
                SqlGenerationHelper.GenerateParameterNamePlaceholder(
                    commandStringBuilder, useOriginalValue
                        ? columnModification.OriginalParameterName!
                        : columnModification.ParameterName!,
                    // ONLY CHANGE
                    columnModification.TypeMapping);
            }
        }
    }

    protected override ResultSetMapping AppendSelectAffectedCountCommand(StringBuilder commandStringBuilder, string name, string? schema, int commandPosition)
    {
        return ResultSetMapping.LastInResultSet | ResultSetMapping.ResultSetWithRowsAffectedOnly;
    }
}

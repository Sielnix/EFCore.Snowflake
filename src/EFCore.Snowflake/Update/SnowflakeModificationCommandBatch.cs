using EFCore.Snowflake.Storage.Internal.Mapping;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace EFCore.Snowflake.Update;

internal class SnowflakeModificationCommandBatch : SingularModificationCommandBatch
{
    // todo: try to allow batches where no read is applied, as in sql server
    private List<IReadOnlyModificationCommand>? _additionalModificationCommands = null;

    public SnowflakeModificationCommandBatch(ModificationCommandBatchFactoryDependencies dependencies)
        : base(dependencies)
    {
        UpdateSqlGenerator = (SnowflakeUpdateSqlGenerator)dependencies.UpdateSqlGenerator;
    }

    public override IReadOnlyList<IReadOnlyModificationCommand> ModificationCommands
    {
        get
        {
            IReadOnlyList<IReadOnlyModificationCommand> baseCommands = base.ModificationCommands;
            if (_additionalModificationCommands is null)
            {
                return baseCommands;
            }

            if (baseCommands.Count  > 0)
            {
                throw new InvalidOperationException(
                    "Base commands should not be set along with additional commands");
            }

            return _additionalModificationCommands;
        }
    }

    private new SnowflakeUpdateSqlGenerator UpdateSqlGenerator { get; }

    public virtual void SetPostModificationReadCommand(IReadOnlyModificationCommand sourceCommand)
    {
        if (StoreCommand is not null)
        {
            throw new InvalidOperationException(RelationalStrings.ModificationCommandBatchAlreadyComplete);
        }

        Check.DebugAssert(
            SqlBuilder.Length == 0,
            "There is already some command");

        Check.DebugAssert(
            ResultSetMappings.Count == 0,
            "There is already some command");

        if (sourceCommand.StoreStoredProcedure is not null)
        {
            throw new InvalidOperationException(
                "Can not set post-read modification command when it is based on stored procedure");
        }

        int commandPosition = ResultSetMappings.Count;
        bool requiresTransaction;
        EntityState sourceCommandEntityState = sourceCommand.EntityState;
        if (sourceCommandEntityState is EntityState.Modified or EntityState.Added)
        {
            ResultSetMappings.Add(
                UpdateSqlGenerator.AppendPostModificationSelectOperation(SqlBuilder, sourceCommand, commandPosition, out  requiresTransaction));
        }
        else
        {
            throw new InvalidOperationException(
                $"Can not append select when entity state is {sourceCommandEntityState}");
        }

        AddParameters(sourceCommand);
        SetRequiresTransaction(commandPosition > 0 || requiresTransaction);

        _additionalModificationCommands = new(capacity:1)
        {
            sourceCommand
        };
    }

    protected override void Consume(RelationalDataReader reader)
    {
        Check.DebugAssert(
            ResultSetMappings.Count == ModificationCommands.Count,
            $"CommandResultSet.Count of {ResultSetMappings.Count} != ModificationCommands.Count of {ModificationCommands.Count}");

        var commandIndex = 0;

        try
        {
            bool? onResultSet = null;
            var hasOutputParameters = false;

            while (commandIndex < ResultSetMappings.Count)
            {
                var resultSetMapping = ResultSetMappings[commandIndex];

                if (resultSetMapping.HasFlag(ResultSetMapping.HasResultRow))
                {
                    if (onResultSet == false)
                    {
                        throw new InvalidOperationException(RelationalStrings.MissingResultSetWhenSaving);
                    }

                    var lastHandledCommandIndex = resultSetMapping.HasFlag(ResultSetMapping.ResultSetWithRowsAffectedOnly)
                        ? ConsumeResultSetWithRowsAffectedOnly(commandIndex, reader)
                        : ConsumeResultSet(commandIndex, reader);

                    Check.DebugAssert(
                        resultSetMapping.HasFlag(ResultSetMapping.LastInResultSet)
                            ? lastHandledCommandIndex == commandIndex
                            : lastHandledCommandIndex > commandIndex, "Bad handling of ResultSetMapping and command indexing");

                    commandIndex = lastHandledCommandIndex + 1;

                    onResultSet = reader.DbDataReader.NextResult();
                }
                else
                {
                    commandIndex++;
                }

                if (resultSetMapping.HasFlag(ResultSetMapping.HasOutputParameters))
                {
                    hasOutputParameters = true;
                }
            }

            if (onResultSet == true)
            {
                Dependencies.UpdateLogger.UnexpectedTrailingResultSetWhenSaving();
            }

            reader.Close();

            if (hasOutputParameters)
            {
                var parameterCounter = 0;
                IReadOnlyModificationCommand command;

                for (commandIndex = 0; commandIndex < ResultSetMappings.Count; commandIndex++, parameterCounter += ParameterCount(command))
                {
                    command = ModificationCommands[commandIndex];

                    Check.DebugAssert(
                        command.ColumnModifications.All(c => c.UseParameter),
                        "This code assumes all column modifications involve a DbParameter (see counting above)");

                    if (!ResultSetMappings[commandIndex].HasFlag(ResultSetMapping.HasOutputParameters))
                    {
                        continue;
                    }

                    // Note: we assume that the return value is the parameter at position 0, and skip it here for the purpose of calculating
                    // the right baseParameterIndex to pass to PropagateOutputParameters below.
                    var rowsAffectedDbParameter = command.RowsAffectedColumn is IStoreStoredProcedureParameter rowsAffectedParameter
                        ? reader.DbCommand.Parameters[parameterCounter + rowsAffectedParameter.Position]
                        : command.StoreStoredProcedure!.ReturnValue is not null
                            ? reader.DbCommand.Parameters[parameterCounter++]
                            : null;

                    if (rowsAffectedDbParameter is not null)
                    {
                        if (rowsAffectedDbParameter.Value is int rowsAffected)
                        {
                            if (rowsAffected != 1)
                            {
                                ThrowAggregateUpdateConcurrencyException(
                                    reader, commandIndex + 1, expectedRowsAffected: 1, rowsAffected: 0);
                            }
                        }
                        else
                        {
                            throw new InvalidOperationException(
                                RelationalStrings.StoredProcedureRowsAffectedNotPopulated(
                                    command.StoreStoredProcedure!.SchemaQualifiedName));
                        }
                    }

                    command.PropagateOutputParameters(reader.DbCommand.Parameters, parameterCounter);
                }
            }
        }
        catch (Exception ex) when (ex is not DbUpdateException and not OperationCanceledException)
        {
            throw new DbUpdateException(
                RelationalStrings.UpdateStoreException,
                ex,
                ModificationCommands[commandIndex < ModificationCommands.Count ? commandIndex : ModificationCommands.Count - 1].Entries);
        }
    }

    protected override async Task<int> ConsumeResultSetWithRowsAffectedOnlyAsync(int commandIndex, RelationalDataReader reader,
        CancellationToken cancellationToken)
    {
        var expectedRowsAffected = 1;
        while (++commandIndex < ResultSetMappings.Count
               && ResultSetMappings[commandIndex - 1].HasFlag(ResultSetMapping.NotLastInResultSet))
        {
            Check.DebugAssert(
                ResultSetMappings[commandIndex].HasFlag(ResultSetMapping.ResultSetWithRowsAffectedOnly),
                "ResultSetMappings[commandIndex].HasFlag(ResultSetMapping.ResultSetWithRowsAffectedOnly)");

            expectedRowsAffected++;
        }

        int rowsAffected = reader.DbDataReader.RecordsAffected;
        if (rowsAffected != expectedRowsAffected)
        {
            await ThrowAggregateUpdateConcurrencyExceptionAsync(
                reader, commandIndex, expectedRowsAffected, rowsAffected, cancellationToken).ConfigureAwait(false);
        }

        return commandIndex - 1;
    }

    protected override int ConsumeResultSetWithRowsAffectedOnly(int commandIndex, RelationalDataReader reader)
    {
        var expectedRowsAffected = 1;
        while (++commandIndex < ResultSetMappings.Count
               && ResultSetMappings[commandIndex - 1].HasFlag(ResultSetMapping.NotLastInResultSet))
        {
            Check.DebugAssert(
                ResultSetMappings[commandIndex].HasFlag(ResultSetMapping.ResultSetWithRowsAffectedOnly),
                "ResultSetMappings[commandIndex].HasFlag(ResultSetMapping.ResultSetWithRowsAffectedOnly)");

            expectedRowsAffected++;
        }

        int rowsAffected = reader.DbDataReader.RecordsAffected;
        if (rowsAffected != expectedRowsAffected)
        {
            ThrowAggregateUpdateConcurrencyException(reader, commandIndex, expectedRowsAffected, rowsAffected);
        }

        return commandIndex - 1;
    }

    private static int ParameterCount(IReadOnlyModificationCommand command)
    {
        // As a shortcut, if the command uses a stored procedure, return the number of parameters directly from it.
        if (command.StoreStoredProcedure is { } storedProcedure)
        {
            return storedProcedure.Parameters.Count;
        }

        // Otherwise we need to count the total parameters used by all column modifications
        var parameterCount = 0;

        for (var i = 0; i < command.ColumnModifications.Count; i++)
        {
            var columnModification = command.ColumnModifications[i];

            if (columnModification.UseCurrentValueParameter)
            {
                parameterCount++;
            }

            if (columnModification.UseOriginalValueParameter)
            {
                parameterCount++;
            }
        }

        return parameterCount;
    }
}

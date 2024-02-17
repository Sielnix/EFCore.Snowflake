using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.Update.Internal;

namespace EFCore.Snowflake.Update.Internal;

public class SnowflakeCommandBatchPreparer : CommandBatchPreparer
{
    public SnowflakeCommandBatchPreparer(CommandBatchPreparerDependencies dependencies)
        : base(dependencies)
    {
        int minBatchSize =
            dependencies.Options.Extensions.OfType<RelationalOptionsExtension>().FirstOrDefault()?.MinBatchSize
            ?? 1;

        if (minBatchSize > 1)
        {
            throw new InvalidOperationException(
                "Minimum batch size is greater than one, which is invalid for Snowflake");
        }
    }

    public override IEnumerable<ModificationCommandBatch> BatchCommands(
       IList<IUpdateEntry> entries,
       IUpdateAdapter updateAdapter)
    {
        var parameterNameGenerator = Dependencies.ParameterNameGeneratorFactory.Create();
        var commands = CreateModificationCommands(entries, updateAdapter, parameterNameGenerator.GenerateNext);
        var commandSets = TopologicalSort(commands);

        for (var commandSetIndex = 0; commandSetIndex < commandSets.Count; commandSetIndex++)
        {
            // overriding this method
            var batches = CreateCommandBatches(
                commandSets[commandSetIndex],
                commandSetIndex < commandSets.Count - 1,
                assertColumnModification: true,
                parameterNameGenerator);

            foreach (var batch in batches)
            {
                yield return batch;
            }
        }
    }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public override IEnumerable<ModificationCommandBatch> CreateCommandBatches(
        IEnumerable<IReadOnlyModificationCommand> commandSet,
        bool moreCommandSets)
        => CreateCommandBatches(commandSet, moreCommandSets, assertColumnModification: false);


    private IEnumerable<ModificationCommandBatch> CreateCommandBatches(
        IEnumerable<IReadOnlyModificationCommand> commandSet,
        bool moreCommandSets,
        bool assertColumnModification,
        ParameterNameGenerator? parameterNameGenerator = null)
    {
        List<IReadOnlyModificationCommand> commandSetList = commandSet.ToList();

        for (int i = 0; i < commandSetList.Count; i++)
        {
            bool isLast = i == commandSetList.Count - 1;

            var modificationCommand = commandSetList[i];
#if DEBUG
            if (assertColumnModification)
            {
                (modificationCommand as ModificationCommand)?.AssertColumnsNotInitialized();
            }
#endif

            if (modificationCommand.EntityState == EntityState.Modified
                && !modificationCommand.ColumnModifications.Any(m => m.IsWrite))
            {
                continue;
            }

            bool requiresSelectCommand = modificationCommand.StoreStoredProcedure == null
                                         && modificationCommand.ColumnModifications.Any(c => c.IsRead);

            SnowflakeModificationCommandBatch batch = StartNewBatch(parameterNameGenerator);

            if (!batch.TryAddCommand(modificationCommand))
            {
                throw new InvalidOperationException("Can not add command to the batch");
            }

            batch.Complete(moreBatchesExpected: requiresSelectCommand || !isLast || moreCommandSets);
            yield return batch;

            // SPECIAL SNOWFLAKE CASE - WE can not Insert/Update & Select in the same one query.
            // We add another batch which selects the requested columns (for example: inserted ID)
            if (requiresSelectCommand)
            {
                batch = StartNewBatch(parameterNameGenerator);
                batch.SetPostModificationReadCommand(modificationCommand);
                batch.Complete(moreBatchesExpected: !isLast || moreCommandSets);

                yield return batch;
            }
        }
    }

    private SnowflakeModificationCommandBatch StartNewBatch(ParameterNameGenerator? parameterNameGenerator)
    {
        parameterNameGenerator?.Reset();

        return (SnowflakeModificationCommandBatch)Dependencies.ModificationCommandBatchFactory.Create();
    }
}

using Microsoft.EntityFrameworkCore.Update;

namespace EFCore.Snowflake.Update;
internal class SnowflakeModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;

    public SnowflakeModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public ModificationCommandBatch Create()
    {
        return new SnowflakeModificationCommandBatch(_dependencies);
    }
}

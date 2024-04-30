using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EFCore.Snowflake.Metadata.Conventions;

public class SnowflakeSequenceOrderConvention : IModelFinalizingConvention
{
    public void ProcessModelFinalizing(IConventionModelBuilder modelBuilder, IConventionContext<IConventionModelBuilder> context)
    {
        IEnumerable<IConventionSequence> sequences = modelBuilder.Metadata.GetSequences();
        foreach (var conventionSequence in sequences)
        {
            conventionSequence.Builder.IsOrdered(true);
        }
    }
}

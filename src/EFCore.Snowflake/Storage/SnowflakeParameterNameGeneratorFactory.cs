using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage;
internal class SnowflakeParameterNameGeneratorFactory : ParameterNameGeneratorFactory
{
    public SnowflakeParameterNameGeneratorFactory(ParameterNameGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    public override ParameterNameGenerator Create()
    {
        //return new SnowflakeParameterNameGenerator();
        return new ParameterNameGenerator();
    }

    private sealed class SnowflakeParameterNameGenerator : ParameterNameGenerator
    {
        private int _count = 1;

        public override string GenerateNext()
        {
            int val = _count;
            _count++;
            return val.ToString(CultureInfo.InvariantCulture);
        }

        public override void Reset()
        {
            _count = 1;
        }
    }
}

using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;
public class ConvertToProviderTypesSnowflakeTest : ConvertToProviderTypesTestBase<
    ConvertToProviderTypesSnowflakeTest.ConvertToProviderTypesSnowflakeFixture>
{
    public ConvertToProviderTypesSnowflakeTest(ConvertToProviderTypesSnowflakeFixture fixture)
        : base(fixture)
    {
    }

    public class ConvertToProviderTypesSnowflakeFixture : ConvertToProviderTypesFixtureBase
    {
        public override bool StrictEquality
            => true;

        public override bool SupportsAnsi
            => false;

        public override bool SupportsUnicodeToAnsiConversion
            => false;

        public override bool SupportsLargeStringComparisons
            => true;

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        public override bool SupportsBinaryKeys
            => true;

        public override bool SupportsDecimalComparisons
            => true;

        public override DateTime DefaultDateTime
            => new();

        public override bool PreservesDateTimeKind
            => false;
    }
}

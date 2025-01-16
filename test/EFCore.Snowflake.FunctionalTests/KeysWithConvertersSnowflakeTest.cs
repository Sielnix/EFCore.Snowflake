using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;
public class KeysWithConvertersSnowflakeTest(KeysWithConvertersSnowflakeTest.KeysWithConvertersSnowflakeFixture fixture)
    : KeysWithConvertersTestBase<
        KeysWithConvertersSnowflakeTest.KeysWithConvertersSnowflakeFixture>(fixture)
{
    public class KeysWithConvertersSnowflakeFixture : KeysWithConvertersFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
            => builder.UseSnowflake(b => b.MinBatchSize(1));
    }

    [ConditionalFact(Skip = "Requires FK index - use Hybrid tables")]
    public override Task Can_insert_and_read_back_with_comparable_struct_binary_key_and_required_dependents()
    {
        return base.Can_insert_and_read_back_with_comparable_struct_binary_key_and_required_dependents();
    }

    [ConditionalFact(Skip = "Requires FK index - use Hybrid tables")]
    public override Task Can_insert_and_read_back_with_comparable_struct_key_and_required_dependents()
    {
        return base.Can_insert_and_read_back_with_comparable_struct_key_and_required_dependents();
    }

    [ConditionalFact(Skip = "Requires FK index - use Hybrid tables")]
    public override Task Can_insert_and_read_back_with_generic_comparable_struct_binary_key_and_required_dependents()
    {
        return base.Can_insert_and_read_back_with_generic_comparable_struct_binary_key_and_required_dependents();
    }

    [ConditionalFact(Skip = "Requires FK index - use Hybrid tables")]
    public override Task Can_insert_and_read_back_with_generic_comparable_struct_key_and_required_dependents()
    {
        return base.Can_insert_and_read_back_with_generic_comparable_struct_key_and_required_dependents();
    }

    [ConditionalFact(Skip = "Requires FK index - use Hybrid tables")]
    public override Task Can_insert_and_read_back_with_struct_binary_key_and_required_dependents()
    {
        return base.Can_insert_and_read_back_with_struct_binary_key_and_required_dependents();
    }

    [ConditionalFact(Skip = "Requires FK index - use Hybrid tables")]
    public override Task Can_insert_and_read_back_with_struct_key_and_required_dependents()
    {
        return base.Can_insert_and_read_back_with_struct_key_and_required_dependents();
    }

    [ConditionalFact(Skip = "Requires FK index - use Hybrid tables")]
    public override Task Can_insert_and_read_back_with_structural_struct_binary_key_and_required_dependents()
    {
        return base.Can_insert_and_read_back_with_structural_struct_binary_key_and_required_dependents();
    }
}

using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Query;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Snowflake.Data.Client;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class ComplexNavigationsQuerySnowflakeTest : ComplexNavigationsQueryRelationalTestBase<ComplexNavigationsQuerySnowflakeFixture>
{
    public ComplexNavigationsQuerySnowflakeTest(
        ComplexNavigationsQuerySnowflakeFixture fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task Collection_FirstOrDefault_property_accesses_in_projection(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Collection_FirstOrDefault_property_accesses_in_projection(async));
    }

    public override async Task Contains_with_subquery_optional_navigation_and_constant_item(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Contains_with_subquery_optional_navigation_and_constant_item(async));
    }

    public override Task GroupJoin_client_method_in_OrderBy(bool async)
        => AssertTranslationFailedWithDetails(
            () => base.GroupJoin_client_method_in_OrderBy(async),
            CoreStrings.QueryUnableToTranslateMethod(
                "Microsoft.EntityFrameworkCore.Query.ComplexNavigationsQueryTestBase<EFCore.Snowflake.FunctionalTests.Query.ComplexNavigationsQuerySnowflakeFixture>",
                "ClientMethodNullableInt"));
    
    public override async Task Join_with_result_selector_returning_queryable_throws_validation_error(bool async)
    {
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await base.Join_with_result_selector_returning_queryable_throws_validation_error(async));
    }

    public override async Task Let_let_contains_from_outer_let(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Let_let_contains_from_outer_let(async));
    }

    public override async Task Member_pushdown_chain_3_levels_deep(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Member_pushdown_chain_3_levels_deep(async));
    }

    public override async Task Member_pushdown_with_collection_navigation_in_the_middle(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Member_pushdown_with_collection_navigation_in_the_middle(async));
    }

    public override async Task Member_pushdown_with_multiple_collections(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Member_pushdown_with_multiple_collections(async));
    }

    public override async Task Multiple_collection_FirstOrDefault_followed_by_member_access_in_projection(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Multiple_collection_FirstOrDefault_followed_by_member_access_in_projection(async));
    }

    public override async Task Nested_SelectMany_correlated_with_join_table_correctly_translated_to_apply(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Nested_SelectMany_correlated_with_join_table_correctly_translated_to_apply(async));
    }

    public override async Task OrderBy_collection_count_ThenBy_reference_navigation(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.OrderBy_collection_count_ThenBy_reference_navigation(async));
    }

    public override async Task Project_collection_navigation_count(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Project_collection_navigation_count(async));
    }

    public override async Task Correlated_projection_with_first(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_projection_with_first(async));
    }

    public override async Task Max_in_multi_level_nested_subquery(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Max_in_multi_level_nested_subquery(async));
    }

    public override async Task Multiple_select_many_in_projection(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Multiple_select_many_in_projection(async));
    }

    public override async Task Single_select_many_in_projection_with_take(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Single_select_many_in_projection_with_take(async));
    }

    private void AssertSql(params string[] expected)
    {
        Fixture.TestSqlLoggerFactory.AssertSql(expected);
    }
}

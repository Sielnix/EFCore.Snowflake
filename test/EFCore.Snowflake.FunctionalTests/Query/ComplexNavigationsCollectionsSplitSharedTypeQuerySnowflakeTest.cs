using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Query;
using Microsoft.EntityFrameworkCore.Query;
using Snowflake.Data.Client;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class ComplexNavigationsCollectionsSplitSharedTypeQuerySnowflakeTest
    : ComplexNavigationsCollectionsSplitSharedTypeQueryRelationalTestBase<ComplexNavigationsSharedTypeQuerySnowflakeFixture>
{
    public ComplexNavigationsCollectionsSplitSharedTypeQuerySnowflakeTest(
        ComplexNavigationsSharedTypeQuerySnowflakeFixture fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task Complex_query_with_let_collection_projection_FirstOrDefault(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Complex_query_with_let_collection_projection_FirstOrDefault(async));
    }

    public override async Task Complex_query_with_let_collection_projection_FirstOrDefault_with_ToList_on_inner_and_outer(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Complex_query_with_let_collection_projection_FirstOrDefault_with_ToList_on_inner_and_outer(async));
    }

    public override async Task Filtered_include_and_non_filtered_include_followed_by_then_include_on_same_navigation(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Filtered_include_and_non_filtered_include_followed_by_then_include_on_same_navigation(async));
    }

    public override async Task Filtered_include_multiple_multi_level_includes_with_first_level_using_filter_include_on_one_of_the_chains_only(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Filtered_include_multiple_multi_level_includes_with_first_level_using_filter_include_on_one_of_the_chains_only(async));
    }

    public override async Task Filtered_include_same_filter_set_on_same_navigation_twice_followed_by_ThenIncludes(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Filtered_include_same_filter_set_on_same_navigation_twice_followed_by_ThenIncludes(async));
    }

    public override async Task Filtered_include_Skip_Take_with_another_Skip_Take_on_top_level(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Filtered_include_Skip_Take_with_another_Skip_Take_on_top_level(async));
    }

    public override async Task Filtered_include_Take_with_another_Take_on_top_level(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Filtered_include_Take_with_another_Take_on_top_level(async));
    }

    public override async Task Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_FirstOrDefault_on_top_level(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_FirstOrDefault_on_top_level(async));
    }

    public override async Task Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_unordered_Take_on_top_level(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_unordered_Take_on_top_level(async));
    }

    public override async Task SelectMany_with_predicate_and_DefaultIfEmpty_projecting_root_collection_element_and_another_collection(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.SelectMany_with_predicate_and_DefaultIfEmpty_projecting_root_collection_element_and_another_collection(async));
    }

    public override async Task Skip_Take_Distinct_on_grouping_element(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Skip_Take_Distinct_on_grouping_element(async));
    }

    public override async Task Skip_Take_on_grouping_element_with_reference_include(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Skip_Take_on_grouping_element_with_reference_include(async));
    }

    public override async Task Skip_Take_Select_collection_Skip_Take(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Skip_Take_Select_collection_Skip_Take(async));
    }

    public override async Task Take_Select_collection_Take(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Take_Select_collection_Take(async));
    }

    private void AssertSql(params string[] expected)
    {
        Fixture.TestSqlLoggerFactory.AssertSql(expected);
    }
}

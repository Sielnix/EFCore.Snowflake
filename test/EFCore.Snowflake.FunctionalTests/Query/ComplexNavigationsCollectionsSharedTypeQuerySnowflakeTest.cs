using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Query;
using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Query;

public class ComplexNavigationsCollectionsSharedTypeQuerySnowflakeTest
    : ComplexNavigationsCollectionsSharedTypeQueryRelationalTestBase<ComplexNavigationsSharedTypeQuerySnowflakeFixture>
{
    public ComplexNavigationsCollectionsSharedTypeQuerySnowflakeTest(
        ComplexNavigationsSharedTypeQuerySnowflakeFixture fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }


    //public override async Task Complex_query_issue_21665(bool async)
    //{
    //    await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () => await base.Complex_query_issue_21665(async));
    //}

    public override async Task Complex_query_with_let_collection_projection_FirstOrDefault(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Complex_query_with_let_collection_projection_FirstOrDefault(async));
    }

    public override async Task Complex_query_with_let_collection_projection_FirstOrDefault_with_ToList_on_inner_and_outer(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Complex_query_with_let_collection_projection_FirstOrDefault_with_ToList_on_inner_and_outer(async));
    }

    public override async Task Filtered_include_after_different_filtered_include_different_level(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Filtered_include_after_different_filtered_include_different_level(async));
    }

    public override async Task Filtered_include_and_non_filtered_include_followed_by_then_include_on_same_navigation(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Filtered_include_and_non_filtered_include_followed_by_then_include_on_same_navigation(async));
    }

    public override async Task Filtered_include_complex_three_level_with_middle_having_filter1(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Filtered_include_complex_three_level_with_middle_having_filter1(async));
    }

    public override async Task Filtered_include_complex_three_level_with_middle_having_filter2(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Filtered_include_complex_three_level_with_middle_having_filter2(async));
    }

    public override async Task Filtered_include_multiple_multi_level_includes_with_first_level_using_filter_include_on_one_of_the_chains_only(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Filtered_include_multiple_multi_level_includes_with_first_level_using_filter_include_on_one_of_the_chains_only(async));
    }

    //public override async Task Filtered_include_outer_parameter_used_inside_filter(bool async)
    //{
    //    await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
    //        await base.Filtered_include_outer_parameter_used_inside_filter(async));
    //}

    public override async Task Filtered_include_same_filter_set_on_same_navigation_twice_followed_by_ThenIncludes(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Filtered_include_same_filter_set_on_same_navigation_twice_followed_by_ThenIncludes(async));
    }

    public override async Task Filtered_include_Skip_Take_with_another_Skip_Take_on_top_level(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Filtered_include_Skip_Take_with_another_Skip_Take_on_top_level(async));
    }

    public override async Task Filtered_include_Take_with_another_Take_on_top_level(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Filtered_include_Take_with_another_Take_on_top_level(async));
    }

    public override async Task Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_FirstOrDefault_on_top_level(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_FirstOrDefault_on_top_level(async));
    }

    public override async Task Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_unordered_Take_on_top_level(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Filtered_include_with_Take_without_order_by_followed_by_ThenInclude_and_unordered_Take_on_top_level(async));
    }

    //public override async Task Include_inside_subquery(bool async)
    //{
    //    await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
    //        await base.Include_inside_subquery(async));
    //}

    public override async Task Projecting_collection_after_optional_reference_correlated_with_parent(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Projecting_collection_after_optional_reference_correlated_with_parent(async));
    }

    public override async Task Projecting_collection_with_group_by_after_optional_reference_correlated_with_parent(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Projecting_collection_with_group_by_after_optional_reference_correlated_with_parent(async));
    }

    public override async Task Skip_Take_Distinct_on_grouping_element(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Skip_Take_Distinct_on_grouping_element(async));
    }

    public override async Task Skip_Take_on_grouping_element_inside_collection_projection(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Skip_Take_on_grouping_element_inside_collection_projection(async));
    }

    public override async Task Skip_Take_on_grouping_element_with_collection_include(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Skip_Take_on_grouping_element_with_collection_include(async));
    }

    public override async Task Skip_Take_on_grouping_element_with_reference_include(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Skip_Take_on_grouping_element_with_reference_include(async));
    }

    public override async Task Skip_Take_Select_collection_Skip_Take(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Skip_Take_Select_collection_Skip_Take(async));
    }

    public override async Task Take_Select_collection_Take(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Take_Select_collection_Take(async));
    }

    public override async Task
        SelectMany_with_predicate_and_DefaultIfEmpty_projecting_root_collection_element_and_another_collection(
            bool async)
        => await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base
                .SelectMany_with_predicate_and_DefaultIfEmpty_projecting_root_collection_element_and_another_collection(
                    async));

    private void AssertSql(params string[] expected)
    {
        Fixture.TestSqlLoggerFactory.AssertSql(expected);
    }
}

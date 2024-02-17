using EFCore.Snowflake.Query;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestModels.GearsOfWarModel;
using Snowflake.Data.Client;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace EFCore.Snowflake.FunctionalTests.Query;
public class GearsOfWarQuerySnowflakeTest : GearsOfWarQueryRelationalTestBase<GearsOfWarQuerySnowflakeFixture>
{
    public GearsOfWarQuerySnowflakeTest(GearsOfWarQuerySnowflakeFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task Conditional_expression_with_test_being_simplified_to_constant_complex(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Conditional_expression_with_test_being_simplified_to_constant_complex(isAsync));
    }

    public override async Task Correlated_collection_after_distinct_3_levels(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collection_after_distinct_3_levels(isAsync));
    }

    public override async Task Correlated_collection_via_SelectMany_with_Distinct_missing_indentifying_columns_in_projection(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collection_via_SelectMany_with_Distinct_missing_indentifying_columns_in_projection(isAsync));
    }

    public override async Task Correlated_collection_with_distinct_not_projecting_identifier_column(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collection_with_distinct_not_projecting_identifier_column(isAsync));
    }

    public override async Task Correlated_collection_with_distinct_projecting_identifier_column(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collection_with_distinct_projecting_identifier_column(isAsync));
    }

    public override async Task Correlated_collection_with_groupby_not_projecting_identifier_column_but_only_grouping_key_in_final_projection(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collection_with_groupby_not_projecting_identifier_column_but_only_grouping_key_in_final_projection(isAsync));
    }

    public override async Task Correlated_collection_with_groupby_not_projecting_identifier_column_with_group_aggregate_in_final_projection(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collection_with_groupby_not_projecting_identifier_column_with_group_aggregate_in_final_projection(isAsync));
    }

    public override async Task Correlated_collection_with_groupby_not_projecting_identifier_column_with_group_aggregate_in_final_projection_multiple_grouping_keys(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collection_with_groupby_not_projecting_identifier_column_with_group_aggregate_in_final_projection_multiple_grouping_keys(isAsync));
    }

    public override async Task Correlated_collection_with_groupby_with_complex_grouping_key_not_projecting_identifier_column_with_group_aggregate_in_final_projection(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collection_with_groupby_with_complex_grouping_key_not_projecting_identifier_column_with_group_aggregate_in_final_projection(isAsync));
    }

    public override async Task Correlated_collection_with_inner_collection_references_element_two_levels_up(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collection_with_inner_collection_references_element_two_levels_up(isAsync));
    }

    public override async Task Correlated_collections_inner_subquery_predicate_references_outer_qsre(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collections_inner_subquery_predicate_references_outer_qsre(isAsync));
    }
    
    public override async Task Correlated_collections_inner_subquery_selector_references_outer_qsre(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collections_inner_subquery_selector_references_outer_qsre(isAsync));
    }

    public override async Task Correlated_collections_nested_inner_subquery_references_outer_qsre_one_level_up(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collections_nested_inner_subquery_references_outer_qsre_one_level_up(isAsync));
    }

    public override async Task Correlated_collections_nested_inner_subquery_references_outer_qsre_two_levels_up(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collections_nested_inner_subquery_references_outer_qsre_two_levels_up(isAsync));
    }

    public override async Task Correlated_collections_with_Distinct(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Correlated_collections_with_Distinct(isAsync));
    }

    public override async Task Correlated_collections_with_FirstOrDefault(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Correlated_collections_with_FirstOrDefault(isAsync));
    }

    [ConditionalTheory(Skip = "Not implemented")]
    [MemberData(nameof(IsAsyncData))]
    public override async  Task Checked_context_with_cast_does_not_fail(bool isAsync)
    {
        //Not implemented
        await base.Checked_context_with_cast_does_not_fail(isAsync);
    }


    [ConditionalTheory(Skip = "BUG IN .net connector")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task DateTimeOffset_Contains_Less_than_Greater_than(bool async)
    {
        await base.DateTimeOffset_Contains_Less_than_Greater_than(async);
    }


    [ConditionalTheory(Skip = "BUG IN .net connector")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task DateTimeOffset_Date_returns_datetime(bool async)
    {
        await base.DateTimeOffset_Date_returns_datetime(async);
    }

    [ConditionalTheory(Skip = "BUG IN .net connector")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task DateTimeOffset_DateAdd_AddDays(bool async)
    {
        await base.DateTimeOffset_DateAdd_AddDays(async);
    }

    [ConditionalTheory(Skip = "BUG IN .net connector")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task DateTimeOffset_DateAdd_AddHours(bool async)
    {
        await base.DateTimeOffset_DateAdd_AddHours(async);
    }

    [ConditionalTheory(Skip = "BUG IN .net connector")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task DateTimeOffset_DateAdd_AddMilliseconds(bool async)
    {
        await base.DateTimeOffset_DateAdd_AddMilliseconds(async);
    }

    [ConditionalTheory(Skip = "BUG IN .net connector")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task DateTimeOffset_DateAdd_AddMinutes(bool async)
    {
        await base.DateTimeOffset_DateAdd_AddMinutes(async);
    }

    [ConditionalTheory(Skip = "BUG IN .net connector")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task DateTimeOffset_DateAdd_AddMonths(bool async)
    {
        await base.DateTimeOffset_DateAdd_AddMonths(async);
    }

    [ConditionalTheory(Skip = "BUG IN .net connector")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task DateTimeOffset_DateAdd_AddSeconds(bool async)
    {
        await base.DateTimeOffset_DateAdd_AddSeconds(async);
    }

    [ConditionalTheory(Skip = "BUG IN .net connector")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task DateTimeOffset_DateAdd_AddYears(bool async)
    {
        await base.DateTimeOffset_DateAdd_AddYears(async);
    }

    public override Task DateTimeOffsetNow_minus_timespan(bool async)
        => Assert.ThrowsAsync<InvalidOperationException>(() => base.DateTimeOffsetNow_minus_timespan(async));

    public override async Task Filter_on_subquery_projecting_one_value_type_from_empty_collection(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Filter_on_subquery_projecting_one_value_type_from_empty_collection(isAsync));
    }

    public override async Task FirstOrDefault_on_empty_collection_of_DateTime_in_subquery(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.FirstOrDefault_on_empty_collection_of_DateTime_in_subquery(isAsync));
    }

    public override async Task FirstOrDefault_over_int_compared_to_zero(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.FirstOrDefault_over_int_compared_to_zero(isAsync));
    }

    public override async Task Include_collection_with_complex_OrderBy2(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Include_collection_with_complex_OrderBy2(isAsync));
    }

    public override async Task Include_collection_with_complex_OrderBy3(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Include_collection_with_complex_OrderBy3(isAsync));
    }

    public override async Task Include_with_complex_order_by(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Include_with_complex_order_by(isAsync));
    }

    public override async Task Multiple_orderby_with_navigation_expansion_on_one_of_the_order_bys_inside_subquery_complex_orderings(bool isAsync)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Multiple_orderby_with_navigation_expansion_on_one_of_the_order_bys_inside_subquery_complex_orderings(isAsync));
    }

    public override async Task Navigation_based_on_complex_expression4(bool async)
        // Nav expansion. Issue #17782.
        => await Assert.ThrowsAsync<EqualException>(
            () => AssertQuery(
                async,
                ss => from lc1 in ss.Set<Faction>().Select(f => (f is LocustHorde) ? ((LocustHorde)f).Commander : null)
                    from lc2 in ss.Set<LocustLeader>().OfType<LocustCommander>()
                    select (lc1 ?? lc2).DefeatedBy));

    public override async Task Navigation_based_on_complex_expression5(bool async)
        // Nav expansion. Issue #17782.
        => await Assert.ThrowsAsync<EqualException>(
            () => AssertQuery(
                async,
                ss => from lc1 in ss.Set<Faction>().OfType<LocustHorde>().Select(lh => lh.Commander)
                    join lc2 in ss.Set<LocustLeader>().OfType<LocustCommander>() on true equals true
                    select (lc1 ?? lc2).DefeatedBy));

    public override async Task Navigation_based_on_complex_expression6(bool async)
        // Nav expansion. Issue #17782.
        => await Assert.ThrowsAsync<EqualException>(
            () => AssertQuery(
                async,
                ss => from lc1 in ss.Set<Faction>().OfType<LocustHorde>().Select(lh => lh.Commander)
                    join lc2 in ss.Set<LocustLeader>().OfType<LocustCommander>() on true equals true
                    select (lc1.Name == "Queen Myrrah" ? lc1 : lc2).DefeatedBy));


    public override async Task Optional_navigation_with_collection_composite_key(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Optional_navigation_with_collection_composite_key(async));
    }

    public override async Task Outer_parameter_in_group_join_with_DefaultIfEmpty(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Outer_parameter_in_group_join_with_DefaultIfEmpty(async));
    }

    public override async Task Outer_parameter_in_join_key(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Outer_parameter_in_join_key(async));
    }

    public override async Task Outer_parameter_in_join_key_inner_and_outer(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(async () =>
            await base.Outer_parameter_in_join_key_inner_and_outer(async));
    }

    public override async Task Project_one_value_type_converted_to_nullable_from_empty_collection(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Project_one_value_type_converted_to_nullable_from_empty_collection(async));
    }

    public override async Task Project_one_value_type_from_empty_collection(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Project_one_value_type_from_empty_collection(async));
    }
    
    public override async Task Query_with_complex_let_containing_ordering_and_filter_projecting_firstOrDefault_element_of_let(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Query_with_complex_let_containing_ordering_and_filter_projecting_firstOrDefault_element_of_let(async));
    }

    public override async Task Select_subquery_boolean(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_boolean(async));
    }

    public override async Task Select_subquery_boolean_with_pushdown(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_boolean_with_pushdown(async));
    }

    public override async Task Select_subquery_distinct_firstordefault(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_distinct_firstordefault(async));
    }

    public override async Task Select_subquery_distinct_singleordefault_boolean_with_pushdown(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_distinct_singleordefault_boolean_with_pushdown(async));
    }

    public override async Task Select_subquery_distinct_singleordefault_boolean1(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_distinct_singleordefault_boolean1(async));
    }

    public override async Task Select_subquery_distinct_singleordefault_boolean2(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_distinct_singleordefault_boolean2(async));
    }

    public override async Task Select_subquery_int_with_inside_cast_and_coalesce(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_int_with_inside_cast_and_coalesce(async));
    }

    public override async Task Select_subquery_int_with_outside_cast_and_coalesce(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_int_with_outside_cast_and_coalesce(async));
    }

    public override async Task Select_subquery_int_with_pushdown_and_coalesce(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_int_with_pushdown_and_coalesce(async));
    }
    public override async Task Select_subquery_int_with_pushdown_and_coalesce2(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_int_with_pushdown_and_coalesce2(async));
    }

    public override async Task Select_subquery_projecting_single_constant_bool(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_projecting_single_constant_bool(async));
    }

    public override async Task Select_subquery_projecting_single_constant_int(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_projecting_single_constant_int(async));
    }

    public override async Task Select_subquery_projecting_single_constant_string(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Select_subquery_projecting_single_constant_string(async));
    }

    public override async Task SelectMany_predicate_with_non_equality_comparison_with_Take_doesnt_convert_to_join(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.SelectMany_predicate_with_non_equality_comparison_with_Take_doesnt_convert_to_join(async));
    }

    public override async Task Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion(async));
    }

    public override async Task Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion_negated(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Subquery_projecting_non_nullable_scalar_contains_non_nullable_value_doesnt_need_null_expansion_negated(async));
    }

    public override async Task Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion(async));
    }

    public override async Task Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion_negated(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Subquery_projecting_nullable_scalar_contains_nullable_value_needs_null_expansion_negated(async));
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Time_of_day_datetimeoffset(bool async)
    {
        await base.Time_of_day_datetimeoffset(async);
    }

    public override async Task Where_contains_on_navigation_with_composite_keys(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_contains_on_navigation_with_composite_keys(async));
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_DateOnly_AddDays(bool async)
    {
        await base.Where_DateOnly_AddDays(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_DateOnly_AddMonths(bool async)
    {
        await base.Where_DateOnly_AddMonths(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_DateOnly_AddYears(bool async)
    {
        await base.Where_DateOnly_AddYears(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_DateOnly_Day(bool async)
    {
        await base.Where_DateOnly_Day(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_DateOnly_DayOfWeek(bool async)
    {
        await base.Where_DateOnly_DayOfWeek(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_DateOnly_DayOfYear(bool async)
    {
        await base.Where_DateOnly_DayOfYear(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_DateOnly_Month(bool async)
    {
        await base.Where_DateOnly_Month(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_DateOnly_Year(bool async)
    {
        await base.Where_DateOnly_Year(async);
    }

    [ConditionalTheory(Skip = "BUG IN .net connector when reading non full hour offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_datetimeoffset_date_component(bool async)
    {
        await base.Where_datetimeoffset_date_component(async);
    }

    [ConditionalTheory(Skip = "BUG IN .net connector when reading non full hour offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_datetimeoffset_day_component(bool async)
    {
        await base.Where_datetimeoffset_day_component(async);
    }

    [ConditionalTheory(Skip = "BUG IN .net connector when reading non full hour offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_datetimeoffset_dayofyear_component(bool async)
    {
        await base.Where_datetimeoffset_dayofyear_component(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_datetimeoffset_hour_component(bool async)
    {
        await base.Where_datetimeoffset_hour_component(async);
    }

    [ConditionalTheory(Skip = "TODO: implement in SnowflakeDateTimeMemberTranslator")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_datetimeoffset_millisecond_component(bool async)
    {
        await base.Where_datetimeoffset_millisecond_component(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_datetimeoffset_minute_component(bool async)
    {
        await base.Where_datetimeoffset_minute_component(async);
    }

    [ConditionalTheory(Skip = "BUG IN .net connector when reading non full hour offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_datetimeoffset_month_component(bool async)
    {
        await base.Where_datetimeoffset_month_component(async);
    }

    // Not supported by design: we support getting a local DateTime via DateTime.Now (based on PG TimeZone), but there's no way to get a
    // non-UTC DateTimeOffset.
    public override Task Where_datetimeoffset_now(bool async)
        => Assert.ThrowsAsync<InvalidOperationException>(() => base.Where_datetimeoffset_now(async));

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_datetimeoffset_second_component(bool async)
    {
        await base.Where_datetimeoffset_second_component(async);
    }

    public override Task Where_datetimeoffset_utcnow(bool async)
        => Assert.ThrowsAsync<InvalidOperationException>(() => base.Where_datetimeoffset_utcnow(async));

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_datetimeoffset_year_component(bool async)
    {
        await base.Where_datetimeoffset_year_component(async);
    }


    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_equals_method_on_nullable_with_object_overload(bool async)
    {
        await base.Where_equals_method_on_nullable_with_object_overload(async);
    }

    public override async Task Where_subquery_boolean(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_boolean(async));
    }

    public override async Task Where_subquery_boolean_with_pushdown(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_boolean_with_pushdown(async));
    }

    public override async Task Where_subquery_concat_firstordefault_boolean(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_concat_firstordefault_boolean(async));
    }

    public override async Task Where_subquery_distinct_first_boolean(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_distinct_first_boolean(async));
    }

    public override async Task Where_subquery_distinct_firstordefault_boolean(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_distinct_firstordefault_boolean(async));
    }

    public override async Task Where_subquery_distinct_firstordefault_boolean_with_pushdown(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_distinct_firstordefault_boolean_with_pushdown(async));
    }

    public override async Task Where_subquery_distinct_last_boolean(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_distinct_last_boolean(async));
    }

    public override async Task Where_subquery_distinct_lastordefault_boolean(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_distinct_lastordefault_boolean(async));
    }

    public override async Task Where_subquery_distinct_orderby_firstordefault_boolean(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_distinct_orderby_firstordefault_boolean(async));
    }

    public override async Task Where_subquery_distinct_orderby_firstordefault_boolean_with_pushdown(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_distinct_orderby_firstordefault_boolean_with_pushdown(async));
    }

    public override async Task Where_subquery_distinct_singleordefault_boolean_with_pushdown(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_distinct_singleordefault_boolean_with_pushdown(async));
    }

    public override async Task Where_subquery_distinct_singleordefault_boolean1(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_distinct_singleordefault_boolean1(async));
    }

    public override async Task Where_subquery_distinct_singleordefault_boolean2(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_distinct_singleordefault_boolean2(async));
    }

    public override async Task Where_subquery_join_firstordefault_boolean(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_join_firstordefault_boolean(async));
    }

    public override async Task Where_subquery_left_join_firstordefault_boolean(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_left_join_firstordefault_boolean(async));
    }

    public override async Task Where_subquery_union_firstordefault_boolean(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_union_firstordefault_boolean(async));
    }

    public override async Task Where_subquery_with_ElementAt_using_column_as_index(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_with_ElementAt_using_column_as_index(async));
    }

    public override async Task Where_subquery_with_ElementAtOrDefault_equality_to_null_with_composite_key(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
            await base.Where_subquery_with_ElementAtOrDefault_equality_to_null_with_composite_key(async));
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_TimeOnly_Add_TimeSpan(bool async)
    {
        await base.Where_TimeOnly_Add_TimeSpan(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_TimeOnly_AddHours(bool async)
    {
        await base.Where_TimeOnly_AddHours(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_TimeOnly_AddMinutes(bool async)
    {
        await base.Where_TimeOnly_AddMinutes(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_TimeOnly_Hour(bool async)
    {
        await base.Where_TimeOnly_Hour(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_TimeOnly_IsBetween(bool async)
    {
        await base.Where_TimeOnly_IsBetween(async);
    }

    public override async Task Where_TimeOnly_Millisecond(bool async)
    {
        // not implemented
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await base.Where_TimeOnly_Millisecond(async));
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_TimeOnly_Second(bool async)
    {
        await base.Where_TimeOnly_Second(async);
    }

    [ConditionalTheory(Skip = "Bug in .net connector when inserting date time offset")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_TimeOnly_Minute(bool async)
    {
        await base.Where_TimeOnly_Minute(async);
    }

    public override async Task Where_TimeOnly_subtract_TimeOnly(bool async)
    {
        // not implemented
        await Assert.ThrowsAsync<InvalidCastException>(async () =>
            await base.Where_TimeOnly_subtract_TimeOnly(async));
    }

    [ConditionalTheory(Skip = "NOT IMPLEMENTED")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_TimeSpan_Hours(bool async)
    {
        // not implemented
        await Assert.ThrowsAsync<InvalidCastException>(async () =>
            await base.Where_TimeSpan_Hours(async));
    }

    [ConditionalTheory(Skip = "NOT IMPLEMENTED")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_TimeSpan_Minutes(bool async)
    {
        // not implemented
        await Assert.ThrowsAsync<InvalidCastException>(async () =>
            await base.Where_TimeSpan_Minutes(async));
    }

    [ConditionalTheory(Skip = "NOT IMPLEMENTED")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Where_TimeSpan_Seconds(bool async)
    {
        // not implemented
        await Assert.ThrowsAsync<InvalidCastException>(async () =>
            await base.Where_TimeSpan_Seconds(async));
    }


    public override async Task Where_TimeSpan_Milliseconds(bool async)
    {
        // not implemented
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await base.Where_TimeSpan_Milliseconds(async));
    }

    protected override bool CanExecuteQueryString => false;
}

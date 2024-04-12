using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Query;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Query;

/// <summary>
/// WARNING - we are overriding ComplexTypeQueryTestBase, not ComplexTypeQueryRelationalTestBase as we meant to.
/// It is because ComplexTypeQueryRelationalTestBase have assertions using wrong version of xunit.
/// This class copies the content of ComplexTypeQueryRelationalTestBase
/// </summary>
public class ComplexTypeQuerySnowflakeTest : ComplexTypeQueryTestBase<ComplexTypeQuerySnowflakeTest.ComplexTypeQuerySnowflakeFixture>
//ComplexTypeQueryRelationalTestBase<ComplexTypeQuerySnowflakeTest.ComplexTypeQuerySnowflakeFixture>

{
    public ComplexTypeQuerySnowflakeTest(ComplexTypeQuerySnowflakeFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task Subquery_over_complex_type(bool async)
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => base.Subquery_over_complex_type(async));

        Assert.Equal(RelationalStrings.SubqueryOverComplexTypesNotSupported("Customer.ShippingAddress#Address"), exception.Message);

        AssertSql();
    }

    public override async Task Concat_two_different_complex_type(bool async)
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => base.Concat_two_different_complex_type(async));

        Assert.Equal(
            RelationalStrings.SetOperationOverDifferentStructuralTypes(
                "Customer.ShippingAddress#Address", "Customer.BillingAddress#Address"), exception.Message);

        AssertSql();
    }

    public override async Task Union_two_different_complex_type(bool async)
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => base.Union_two_different_complex_type(async));

        Assert.Equal(
            RelationalStrings.SetOperationOverDifferentStructuralTypes(
                "Customer.ShippingAddress#Address", "Customer.BillingAddress#Address"), exception.Message);

        AssertSql();
    }

    public override async Task Complex_type_equals_null(bool async)
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => base.Complex_type_equals_null(async));

        Assert.Equal(RelationalStrings.CannotCompareComplexTypeToNull, exception.Message);

        AssertSql();
    }

    public override async Task Subquery_over_struct_complex_type(bool async)
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => base.Subquery_over_struct_complex_type(async));

        Assert.Equal(
            RelationalStrings.SubqueryOverComplexTypesNotSupported("ValuedCustomer.ShippingAddress#AddressStruct"), exception.Message);

        AssertSql();
    }

    public override async Task Concat_two_different_struct_complex_type(bool async)
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => base.Concat_two_different_struct_complex_type(async));

        Assert.Equal(
            RelationalStrings.SetOperationOverDifferentStructuralTypes(
                "ValuedCustomer.ShippingAddress#AddressStruct", "ValuedCustomer.BillingAddress#AddressStruct"), exception.Message);

        AssertSql();
    }

    public override async Task Union_two_different_struct_complex_type(bool async)
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => base.Union_two_different_struct_complex_type(async));

        Assert.Equal(
            RelationalStrings.SetOperationOverDifferentStructuralTypes(
                "ValuedCustomer.ShippingAddress#AddressStruct", "ValuedCustomer.BillingAddress#AddressStruct"), exception.Message);

        AssertSql();
    }

    public override async Task
        Same_entity_with_complex_type_projected_twice_with_pushdown_as_part_of_another_projection(bool async)
    {
        await Assert.ThrowsAsync<SnowflakeOuterApplyNotSupportedException>(() => base.Same_entity_with_complex_type_projected_twice_with_pushdown_as_part_of_another_projection(async));
    }

    // This test fails because when OptionalCustomer is null, we get all-null results because of the LEFT JOIN, and we materialize this
    // as an empty ShippingAddress instead of null (see SQL). The proper solution here would be to project the Customer ID just for the
    // purpose of knowing that it's there.
    public override async Task Project_complex_type_via_optional_navigation(bool async)
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => base.Project_complex_type_via_optional_navigation(async));

        Assert.Equal(RelationalStrings.CannotProjectNullableComplexType("Customer.ShippingAddress#Address"), exception.Message);
    }

    // This test fails because when OptionalCustomer is null, we get all-null results because of the LEFT JOIN, and we materialize this
    // as an empty ShippingAddress instead of null (see SQL). The proper solution here would be to project the Customer ID just for the
    // purpose of knowing that it's there.
    public override async Task Project_struct_complex_type_via_optional_navigation(bool async)
    {
        var exception =
            await Assert.ThrowsAsync<InvalidOperationException>(() => base.Project_struct_complex_type_via_optional_navigation(async));

        Assert.Equal(RelationalStrings.CannotProjectNullableComplexType("ValuedCustomer.ShippingAddress#AddressStruct"), exception.Message);
    }

    public class ComplexTypeQuerySnowflakeFixture : ComplexTypeQueryRelationalFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory => SnowflakeTestStoreFactory.Instance;
    }

    private void AssertSql(params string[] expected)
    {
        Fixture.TestSqlLoggerFactory.AssertSql(expected);
    }
}

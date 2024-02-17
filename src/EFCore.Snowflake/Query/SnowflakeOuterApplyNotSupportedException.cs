namespace EFCore.Snowflake.Query;

public class SnowflakeOuterApplyNotSupportedException() : InvalidOperationException(ExceptionMessage)
{
    private const string ExceptionMessage =
        "Outer apply is not supported in Snowflake. Perhaps you could execute query using split queries by using .AsSplitQuery() on IQueryable<> object.";
}

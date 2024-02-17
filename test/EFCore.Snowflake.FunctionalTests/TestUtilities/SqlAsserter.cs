using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests.TestUtilities;
internal static class SqlAsserter
{
    public static void AssertSql(this TestSqlLoggerFactory testSqlLoggerFactory, params string[] expected)
    {
        int tops = Math.Min(expected.Length, testSqlLoggerFactory.SqlStatements.Count);
        for (int i = 0; i < tops; i++)
        {
            string expectedQ = expected[i].Trim().Replace("\r\n", "\n").Replace("\n", "\r\n");
            string executedQ = testSqlLoggerFactory.SqlStatements[i].Trim();

            Assert.Equal(expectedQ, executedQ);
        }

        if (expected.Length != testSqlLoggerFactory.SqlStatements.Count)
        {
            Assert.Fail("Invalid query count");
        }
    }
}

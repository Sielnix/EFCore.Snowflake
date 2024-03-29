using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Infrastructure;

namespace EFCore.Snowflake.FunctionalTests;

public abstract class CommandInterceptionSnowflakeTestBase(CommandInterceptionSnowflakeTestBase.InterceptionSnowflakeFixtureBase fixture)
    : CommandInterceptionTestBase(fixture)
{
    public abstract class InterceptionSnowflakeFixtureBase : InterceptionFixtureBase
    {
        protected override string StoreName
            => "CommandInterception";

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        protected override IServiceCollection InjectInterceptors(
            IServiceCollection serviceCollection,
            IEnumerable<IInterceptor> injectedInterceptors)
            => base.InjectInterceptors(serviceCollection.AddEntityFrameworkSnowflake(), injectedInterceptors);
    }

    public class CommandInterceptionSnowflakeTest(CommandInterceptionSnowflakeTest.InterceptionSnowflakeFixture fixture)
        : CommandInterceptionSnowflakeTestBase(fixture), IClassFixture<CommandInterceptionSnowflakeTest.InterceptionSnowflakeFixture>
    {
        public class InterceptionSnowflakeFixture : InterceptionSnowflakeFixtureBase
        {
            protected override bool ShouldSubscribeToDiagnosticListener
                => false;
        }
    }

    public class CommandInterceptionWithDiagnosticsSnowflakeTest(
        CommandInterceptionWithDiagnosticsSnowflakeTest.InterceptionSnowflakeFixture fixture)
        : CommandInterceptionSnowflakeTestBase(fixture), IClassFixture<CommandInterceptionWithDiagnosticsSnowflakeTest.InterceptionSnowflakeFixture>
    {
        public class InterceptionSnowflakeFixture : InterceptionSnowflakeFixtureBase
        {
            protected override bool ShouldSubscribeToDiagnosticListener
                => true;
        }
    }
}

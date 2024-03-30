using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace EFCore.Snowflake.FunctionalTests;

public abstract class ConnectionInterceptionSnowflakeTestBase : ConnectionInterceptionTestBase
{
    protected ConnectionInterceptionSnowflakeTestBase(InterceptionSnowflakeFixtureBase fixture)
        : base(fixture)
    {
    }

    public abstract class InterceptionSnowflakeFixtureBase : InterceptionFixtureBase
    {
        protected override string StoreName
            => "ConnectionInterception";

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        protected override IServiceCollection InjectInterceptors(
            IServiceCollection serviceCollection,
            IEnumerable<IInterceptor> injectedInterceptors)
            => base.InjectInterceptors(serviceCollection.AddEntityFrameworkSnowflake(), injectedInterceptors);
    }

    protected override DbContextOptionsBuilder ConfigureProvider(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseSnowflake();

    protected override BadUniverseContext CreateBadUniverse(DbContextOptionsBuilder optionsBuilder)
        => new(optionsBuilder.UseSnowflake(new FakeDbConnection()).Options);

    public class FakeDbConnection : DbConnection
    {
        [AllowNull]
        public override string ConnectionString { get; set; }

        public override string Database
            => "Database";

        public override string DataSource
            => "DataSource";

        public override string ServerVersion
            => throw new NotImplementedException();

        public override ConnectionState State
            => ConnectionState.Closed;

        public override void ChangeDatabase(string databaseName)
            => throw new NotImplementedException();

        public override void Close()
            => throw new NotImplementedException();

        public override void Open()
            => throw new NotImplementedException();

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => throw new NotImplementedException();

        protected override DbCommand CreateDbCommand()
            => throw new NotImplementedException();
    }

    public class ConnectionInterceptionSnowflakeTest
        : ConnectionInterceptionSnowflakeTestBase, IClassFixture<ConnectionInterceptionSnowflakeTest.InterceptionSnowflakeFixture>
    {
        public ConnectionInterceptionSnowflakeTest(InterceptionSnowflakeFixture fixture)
            : base(fixture)
        {
        }

        public class InterceptionSnowflakeFixture : InterceptionSnowflakeFixtureBase
        {
            protected override bool ShouldSubscribeToDiagnosticListener
                => false;
        }
    }

    public class ConnectionInterceptionWithConnectionStringSnowflakeTest
        : ConnectionInterceptionSnowflakeTestBase,
            IClassFixture<ConnectionInterceptionWithConnectionStringSnowflakeTest.InterceptionSnowflakeFixture>
    {
        public ConnectionInterceptionWithConnectionStringSnowflakeTest(InterceptionSnowflakeFixture fixture)
            : base(fixture)
        {
        }

        public class InterceptionSnowflakeFixture : InterceptionSnowflakeFixtureBase
        {
            protected override bool ShouldSubscribeToDiagnosticListener
                => false;
        }

        protected override DbContextOptionsBuilder ConfigureProvider(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseSnowflake("Database=Dummy");
    }

    public class ConnectionInterceptionWithDiagnosticsSnowflakeTest
        : ConnectionInterceptionSnowflakeTestBase,
            IClassFixture<ConnectionInterceptionWithDiagnosticsSnowflakeTest.InterceptionSnowflakeFixture>
    {
        public ConnectionInterceptionWithDiagnosticsSnowflakeTest(InterceptionSnowflakeFixture fixture)
            : base(fixture)
        {
        }

        public class InterceptionSnowflakeFixture : InterceptionSnowflakeFixtureBase
        {
            protected override bool ShouldSubscribeToDiagnosticListener
                => true;
        }
    }
}

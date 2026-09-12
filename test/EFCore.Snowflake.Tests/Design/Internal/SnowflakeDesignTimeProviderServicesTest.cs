using System.Reflection;
using EFCore.Snowflake.Design.Internal;
using EFCore.Snowflake.Storage.Internal;
using Microsoft.EntityFrameworkCore.Design;

namespace EFCore.Snowflake.Tests.Design.Internal;

public class SnowflakeDesignTimeProviderServicesTest
{
    [Fact]
    public void Ensure_assembly_identity_matches()
    {
        var runtimeAssembly = typeof(SnowflakeConnection).Assembly;
        var dtAttribute = runtimeAssembly.GetCustomAttribute<DesignTimeProviderServicesAttribute>();
        var dtType = typeof(SnowflakeDesignTimeServices);
        Assert.NotNull(dtType);

        Assert.NotNull(dtAttribute);
        Assert.Equal(dtType.FullName, dtAttribute.TypeName);
    }
}

using EFCore.Snowflake.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using System.Reflection;

namespace EFCore.Snowflake.Scaffolding.Internal;

public class SnowflakeCodeGenerator : ProviderCodeGenerator
{
    private static readonly MethodInfo UseSnowflakeMethodInfo
        = typeof(SnowflakeDbContextOptionsBuilderExtensions).GetRuntimeMethod(
            nameof(SnowflakeDbContextOptionsBuilderExtensions.UseSnowflake),
            [typeof(DbContextOptionsBuilder), typeof(string), typeof(Action<SnowflakeDbContextOptionsBuilder>)])!;

    public SnowflakeCodeGenerator(ProviderCodeGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    public override MethodCallCodeFragment GenerateUseProvider(string connectionString, MethodCallCodeFragment? providerOptions)
        => new(
            UseSnowflakeMethodInfo,
            providerOptions == null
                ? [connectionString]
                : [connectionString, new NestedClosureCodeFragment("x", providerOptions)]);
}

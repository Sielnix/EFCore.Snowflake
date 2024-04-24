using EFCore.Snowflake.Infrastructure;
using EFCore.Snowflake.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using System.Reflection;

namespace EFCore.Snowflake.Tests.Scaffolding.Internal;

public class SnowflakeCodeGeneratorTest
{
    [ConditionalFact]
    public virtual void Use_provider_method_is_generated_correctly()
    {
        var codeGenerator = new SnowflakeCodeGenerator(
            new ProviderCodeGeneratorDependencies(
                Enumerable.Empty<IProviderCodeGeneratorPlugin>()));

        var result = codeGenerator.GenerateUseProvider("Data Source=Test", providerOptions: null);

        Assert.Equal("UseSnowflake", result.Method);
        Assert.Collection(
            result.Arguments,
            a => Assert.Equal("Data Source=Test", a));
        Assert.Null(result.ChainedCall);
    }

    [ConditionalFact]
    public virtual void Use_provider_method_is_generated_correctly_with_options()
    {
        var codeGenerator = new SnowflakeCodeGenerator(
            new ProviderCodeGeneratorDependencies(
                Enumerable.Empty<IProviderCodeGeneratorPlugin>()));

        var providerOptions = new MethodCallCodeFragment(_setProviderOptionMethodInfo);

        var result = codeGenerator.GenerateUseProvider("Data Source=Test", providerOptions);

        Assert.Equal("UseSnowflake", result.Method);
        Assert.Collection(
            result.Arguments,
            a => Assert.Equal("Data Source=Test", a),
            a =>
            {
                var nestedClosure = Assert.IsType<NestedClosureCodeFragment>(a);

                Assert.Equal("x", nestedClosure.Parameter);
                Assert.Same(providerOptions, nestedClosure.MethodCalls[0]);
            });
        Assert.Null(result.ChainedCall);
    }
    
    private static readonly MethodInfo _setProviderOptionMethodInfo
        = typeof(SnowflakeCodeGeneratorTest).GetRuntimeMethod(nameof(SetProviderOption), new[] { typeof(DbContextOptionsBuilder) })!;

    public static SnowflakeDbContextOptionsBuilder SetProviderOption(DbContextOptionsBuilder optionsBuilder)
        => throw new NotSupportedException();
}

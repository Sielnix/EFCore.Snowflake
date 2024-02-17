namespace EFCore.Snowflake.FunctionalTests.TestModels.SemiStructuredTypesModel;
public readonly record struct DummyObject1(long Key, string Value);
public readonly record struct DummyObject2(string Name, string? Description, DummyObject1[] InsideData);

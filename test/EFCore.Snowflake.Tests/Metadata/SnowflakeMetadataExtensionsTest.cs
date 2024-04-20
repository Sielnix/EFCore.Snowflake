using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.Snowflake.Tests.Metadata;

public class SnowflakeMetadataExtensionsTest
{
    [Fact]
    public void Can_get_and_set_column_name()
    {
        var modelBuilder = GetModelBuilder();

        var property = modelBuilder
            .Entity<Customer>()
            .Property(e => e.Name)
            .Metadata;

        Assert.Equal("Name", property.GetColumnName());
        Assert.Null(((IConventionProperty)property).GetColumnNameConfigurationSource());

        ((IConventionProperty)property).SetColumnName("Eman", fromDataAnnotation: true);

        Assert.Equal("Eman", property.GetColumnName());
        Assert.Equal(ConfigurationSource.DataAnnotation, ((IConventionProperty)property).GetColumnNameConfigurationSource());

        property.SetColumnName("MyNameIs");

        Assert.Equal("Name", property.Name);
        Assert.Equal("MyNameIs", property.GetColumnName());
        Assert.Equal(ConfigurationSource.Explicit, ((IConventionProperty)property).GetColumnNameConfigurationSource());

        property.SetColumnName(null);

        Assert.Equal("Name", property.GetColumnName());
        Assert.Null(((IConventionProperty)property).GetColumnNameConfigurationSource());
    }

    [Fact]
    public void Can_get_and_set_column_key_name()
    {
        var modelBuilder = GetModelBuilder();

        var key = modelBuilder
            .Entity<Customer>()
            .HasKey(e => e.Id)
            .Metadata;

        Assert.Equal("PK_Customer", key.GetName());

        key.SetName("PrimaryKey");

        Assert.Equal("PrimaryKey", key.GetName());

        key.SetName("PrimarySchool");

        Assert.Equal("PrimarySchool", key.GetName());

        key.SetName(null);

        Assert.Equal("PK_Customer", key.GetName());
    }

    [Fact]
    public void Can_get_and_set_sequence()
    {
        var modelBuilder = GetModelBuilder();
        var model = modelBuilder.Model;

        Assert.Null(model.FindSequence("Foo"));
        Assert.Null(model.FindSequence("Foo"));
        Assert.Null(((IModel)model).FindSequence("Foo"));

        var sequence = model.AddSequence("Foo");

        Assert.Equal("Foo", model.FindSequence("Foo")!.Name);
        Assert.Equal("Foo", ((IModel)model).FindSequence("Foo")!.Name);
        Assert.Equal("Foo", model.FindSequence("Foo")!.Name);
        Assert.Equal("Foo", ((IModel)model).FindSequence("Foo")!.Name);

        Assert.Equal("Foo", sequence.Name);
        Assert.Null(sequence.Schema);
        Assert.Equal(1, sequence.IncrementBy);
        Assert.Equal(1, sequence.StartValue);
        Assert.Null(sequence.MinValue);
        Assert.Null(sequence.MaxValue);
        Assert.Same(typeof(long), sequence.Type);

        Assert.NotNull(model.FindSequence("Foo"));

        var sequence2 = model.FindSequence("Foo");

        sequence.StartValue = 1729;
        sequence.IncrementBy = 11;
        sequence.MinValue = 2001;
        sequence.MaxValue = 2010;
        sequence.Type = typeof(int);

        Assert.Equal("Foo", sequence.Name);
        Assert.Null(sequence.Schema);
        Assert.Equal(11, sequence.IncrementBy);
        Assert.Equal(1729, sequence.StartValue);
        Assert.Equal(2001, sequence.MinValue);
        Assert.Equal(2010, sequence.MaxValue);
        Assert.Same(typeof(int), sequence.Type);

        Assert.Equal(sequence2!.Name, sequence.Name);
        Assert.Equal(sequence2.Schema, sequence.Schema);
        Assert.Equal(sequence2.IncrementBy, sequence.IncrementBy);
        Assert.Equal(sequence2.StartValue, sequence.StartValue);
        Assert.Equal(sequence2.MinValue, sequence.MinValue);
        Assert.Equal(sequence2.MaxValue, sequence.MaxValue);
        Assert.Same(sequence2.Type, sequence.Type);
    }

    [Fact]
    public void Can_get_and_set_sequence_with_schema_name()
    {
        var modelBuilder = GetModelBuilder();
        var model = modelBuilder.Model;

        Assert.Null(model.FindSequence("Foo", "Smoo"));
        Assert.Null(model.FindSequence("Foo", "Smoo"));
        Assert.Null(((IModel)model).FindSequence("Foo", "Smoo"));

        var sequence = model.AddSequence("Foo", "Smoo");

        Assert.Equal("Foo", model.FindSequence("Foo", "Smoo")!.Name);
        Assert.Equal("Foo", ((IModel)model).FindSequence("Foo", "Smoo")!.Name);
        Assert.Equal("Foo", model.FindSequence("Foo", "Smoo")!.Name);
        Assert.Equal("Foo", ((IModel)model).FindSequence("Foo", "Smoo")!.Name);

        Assert.Equal("Foo", sequence.Name);
        Assert.Equal("Smoo", sequence.Schema);
        Assert.Equal(1, sequence.IncrementBy);
        Assert.Equal(1, sequence.StartValue);
        Assert.Null(sequence.MinValue);
        Assert.Null(sequence.MaxValue);
        Assert.Same(typeof(long), sequence.Type);

        Assert.NotNull(model.FindSequence("Foo", "Smoo"));

        var sequence2 = model.FindSequence("Foo", "Smoo");

        sequence.StartValue = 1729;
        sequence.IncrementBy = 11;
        sequence.MinValue = 2001;
        sequence.MaxValue = 2010;
        sequence.Type = typeof(int);

        Assert.Equal("Foo", sequence.Name);
        Assert.Equal("Smoo", sequence.Schema);
        Assert.Equal(11, sequence.IncrementBy);
        Assert.Equal(1729, sequence.StartValue);
        Assert.Equal(2001, sequence.MinValue);
        Assert.Equal(2010, sequence.MaxValue);
        Assert.Same(typeof(int), sequence.Type);

        Assert.Equal(sequence2!.Name, sequence.Name);
        Assert.Equal(sequence2.Schema, sequence.Schema);
        Assert.Equal(sequence2.IncrementBy, sequence.IncrementBy);
        Assert.Equal(sequence2.StartValue, sequence.StartValue);
        Assert.Equal(sequence2.MinValue, sequence.MinValue);
        Assert.Equal(sequence2.MaxValue, sequence.MaxValue);
        Assert.Same(sequence2.Type, sequence.Type);
    }

    [Fact]
    public void Can_get_multiple_sequences()
    {
        var modelBuilder = GetModelBuilder();
        var model = modelBuilder.Model;

        model.AddSequence("Fibonacci");
        model.AddSequence("Golomb");

        var sequences = model.GetSequences();

        Assert.Equal(2, sequences.Count());
        Assert.Contains(sequences, s => s.Name == "Fibonacci");
        Assert.Contains(sequences, s => s.Name == "Golomb");
    }

    [Fact]
    public void Can_get_multiple_sequences_when_overridden()
    {
        var modelBuilder = GetModelBuilder();
        var model = modelBuilder.Model;

        model.AddSequence("Fibonacci").StartValue = 1;
        model.FindSequence("Fibonacci")!.StartValue = 3;
        model.AddSequence("Golomb");

        var sequences = model.GetSequences();

        Assert.Equal(2, sequences.Count());
        Assert.Contains(sequences, s => s.Name == "Golomb");

        var sequence = sequences.FirstOrDefault(s => s.Name == "Fibonacci");
        Assert.NotNull(sequence);
        Assert.Equal(3, sequence.StartValue);
    }

    [Fact]
    public void Can_get_and_set_value_generation_on_model()
    {
        var modelBuilder = GetModelBuilder();
        var model = modelBuilder.Model;

        // TODO for PG9.6 testing: make this conditional
        Assert.Equal(SnowflakeValueGenerationStrategy.AutoIncrement, model.GetValueGenerationStrategy());

        model.SetValueGenerationStrategy(SnowflakeValueGenerationStrategy.Sequence);

        Assert.Equal(SnowflakeValueGenerationStrategy.Sequence, model.GetValueGenerationStrategy());

        model.SetValueGenerationStrategy(null);

        Assert.Null(model.GetValueGenerationStrategy());
    }
    
    private static ModelBuilder GetModelBuilder()
        => SnowflakeTestHelpers.Instance.CreateConventionBuilder();

    // ReSharper disable once ClassNeverInstantiated.Local
    private class Customer
    {
        public int Id { get; set; }
        public int? NullableInt { get; set; }
        public string Name { get; set; } = null!;
        public byte Byte { get; set; }
        public byte? NullableByte { get; set; }
        public byte[] ByteArray { get; set; } = null!;
    }

    private class Order
    {
        public int OrderId { get; set; }
        public int CustomerId { get; set; }
    }
}

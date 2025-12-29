using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage;

public class SnowflakeRelationalCommandBuilder : RelationalCommandBuilder
{
    private readonly IndentedStringBuilder _commandTextBuilder = new();
    private IndentedStringBuilder? _logCommandTextBuilder;
    
    public SnowflakeRelationalCommandBuilder(RelationalCommandBuilderDependencies dependencies)
        : base(dependencies)
    {
    }

    public override IRelationalCommand Build()
    {
        var commandText = _commandTextBuilder.ToString();
        var logCommandText = _logCommandTextBuilder?.ToString() ?? commandText;
        return new SnowflakeRelationalCommand(Dependencies, commandText, logCommandText, Parameters);
    }

    /**
     * Unfortunately, we have to override all methods below because _logCommandTextBuilder is private in base class.
     */

    public override string ToString()
        => _commandTextBuilder.ToString();

    /// <inheritdoc />
    public override IRelationalCommandBuilder Append(string value, bool sensitive = false)
    {
        InitializeLogCommandTextBuilderIfNeeded(sensitive);
        _commandTextBuilder.Append(value);
        _logCommandTextBuilder?.Append(sensitive ? "?" : value);

        return this;
    }

    /// <inheritdoc />
    public override IRelationalCommandBuilder Append(FormattableString value, bool sensitive = false)
    {
        InitializeLogCommandTextBuilderIfNeeded(sensitive);
        _commandTextBuilder.Append(value);
        _logCommandTextBuilder?.Append(sensitive ? $"?" : value);

        return this;
    }

    /// <inheritdoc />
    public override IRelationalCommandBuilder AppendLine()
    {
        _commandTextBuilder.AppendLine();
        _logCommandTextBuilder?.AppendLine();

        return this;
    }

    /// <inheritdoc />
    public override IRelationalCommandBuilder IncrementIndent()
    {
        _commandTextBuilder.IncrementIndent();
        _logCommandTextBuilder?.IncrementIndent();

        return this;
    }

    /// <inheritdoc />
    public override IRelationalCommandBuilder DecrementIndent()
    {
        _commandTextBuilder.DecrementIndent();
        _logCommandTextBuilder?.DecrementIndent();

        return this;
    }

    /// <inheritdoc />
    public override int CommandTextLength
        => _commandTextBuilder.Length;

    private void InitializeLogCommandTextBuilderIfNeeded(bool sensitive)
    {
        if (sensitive
            && _logCommandTextBuilder is null
            && !Dependencies.LoggingOptions.IsSensitiveDataLoggingEnabled)
        {
            _logCommandTextBuilder = _commandTextBuilder.Clone();
        }
    }
}

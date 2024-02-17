namespace EFCore.Snowflake;

public abstract class SnowflakeEntityFrameworkException : Exception
{
    protected SnowflakeEntityFrameworkException()
    {
    }

    protected SnowflakeEntityFrameworkException(string? message) : base(message)
    {
    }

    protected SnowflakeEntityFrameworkException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}

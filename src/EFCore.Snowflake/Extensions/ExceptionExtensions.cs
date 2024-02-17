namespace EFCore.Snowflake.Extensions;
internal static class ExceptionExtensions
{
    public static IEnumerable<Exception> GetAllExceptions(this Exception exception)
    {
        yield return exception;

        if (exception is AggregateException aggregate)
        {
            foreach (var innerInAggregate in aggregate.InnerExceptions)
            {
                foreach (var inner in GetAllExceptions(innerInAggregate))
                {
                    yield return inner;
                }
            }

            if (aggregate.InnerExceptions.Count == 1)
            {
                yield break;
            }
        }

        if (exception.InnerException is not null)
        {
            foreach (var inner in GetAllExceptions(exception.InnerException))
            {
                yield return inner;
            }
        }
    }
}

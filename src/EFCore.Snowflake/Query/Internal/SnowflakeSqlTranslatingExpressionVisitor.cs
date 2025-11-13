using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using EFCore.Snowflake.Extensions;
using EFCore.Snowflake.Storage.Internal.Mapping;
using EFCore.Snowflake.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Snowflake.Query.Internal;

public class SnowflakeSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    private readonly QueryCompilationContext _queryCompilationContext;
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    private static readonly MethodInfo StringStartsWithMethod
        = typeof(string).GetRuntimeMethod(nameof(string.StartsWith), new[] { typeof(string) })!;

    private static readonly MethodInfo StringEndsWithMethod
        = typeof(string).GetRuntimeMethod(nameof(string.EndsWith), new[] { typeof(string) })!;

    private static readonly MethodInfo StringContainsMethod
        = typeof(string).GetRuntimeMethod(nameof(string.Contains), new[] { typeof(string) })!;

    private static readonly MethodInfo EscapeLikePatternParameterMethod =
        typeof(SnowflakeSqlTranslatingExpressionVisitor).GetTypeInfo().GetDeclaredMethod(nameof(ConstructLikePatternParameter))!;

    private static readonly IReadOnlyDictionary<ExpressionType, IReadOnlyCollection<Type>> RestrictedBinaryExpressions
        = new Dictionary<ExpressionType, IReadOnlyCollection<Type>>
        {
            [ExpressionType.Add] = new HashSet<Type>
            {
                typeof(TimeSpan)
            },
            [ExpressionType.Divide] = new HashSet<Type>
            {
                typeof(TimeSpan),
            },
            [ExpressionType.GreaterThan] = new HashSet<Type>
            {
                typeof(TimeSpan),
            },
            [ExpressionType.GreaterThanOrEqual] = new HashSet<Type>
            {
                typeof(TimeSpan),
            },
            [ExpressionType.LessThan] = new HashSet<Type>
            {
                typeof(TimeSpan),
            },
            [ExpressionType.LessThanOrEqual] = new HashSet<Type>
            {
                typeof(TimeSpan),
            },
            [ExpressionType.Modulo] = new HashSet<Type>
            {
                typeof(TimeSpan)
            },
            [ExpressionType.Multiply] = new HashSet<Type>
            {
                typeof(TimeSpan),
            },
            [ExpressionType.Subtract] = new HashSet<Type>
            {
                typeof(TimeSpan)
            }
        };

    private const char LikeEscapeChar = '\\';
    private const string LikeEscapeCharString = "\\";

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public SnowflakeSqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
        _queryCompilationContext = queryCompilationContext;
        _sqlExpressionFactory = dependencies.SqlExpressionFactory;
    }

    protected override Expression VisitBinary(BinaryExpression binaryExpression)
    {
        Expression baseResult = base.VisitBinary(binaryExpression);

        if (baseResult is SqlBinaryExpression sqlBinary)
        {
            if (RestrictedBinaryExpressions.TryGetValue(sqlBinary.OperatorType, out var restrictedTypes))
            {
                Type leftType = sqlBinary.Left.Type;
                Type rightType = sqlBinary.Right.Type;

                if (restrictedTypes.Contains(leftType) ||
                    restrictedTypes.Contains(rightType))
                {
                    return QueryCompilationContext.NotTranslatedExpression;
                }
            }
        }

        return baseResult;
    }


    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
    {
        var method = methodCallExpression.Method;

        if (method.IsGenericMethod
            && method.GetGenericMethodDefinition() == EnumerableMethods.ElementAt
            && methodCallExpression.Arguments[0].Type == typeof(byte[]))
        {
            return TranslateByteArrayElementAccess(
                methodCallExpression.Arguments[0],
                methodCallExpression.Arguments[1],
                methodCallExpression.Type);
        }

        if (method == StringStartsWithMethod
            && TryTranslateStartsEndsWithContains(
                methodCallExpression.Object!, methodCallExpression.Arguments[0], StartsEndsWithContains.StartsWith, out var translation1))
        {
            return translation1;
        }

        if (method == StringEndsWithMethod
            && TryTranslateStartsEndsWithContains(
                methodCallExpression.Object!, methodCallExpression.Arguments[0], StartsEndsWithContains.EndsWith, out var translation2))
        {
            return translation2;
        }

        if (method == StringContainsMethod
            && TryTranslateStartsEndsWithContains(
                methodCallExpression.Object!, methodCallExpression.Arguments[0], StartsEndsWithContains.Contains, out var translation3))
        {
            return translation3;
        }

        if (method.IsGenericMethod
            && method.GetGenericMethodDefinition() == QueryableMethods.Contains
            && methodCallExpression.Arguments[0] is MethodCallExpression { Method.Name: nameof(Queryable.AsQueryable) } asQueryableCall
            && TryTranslateArrayContains(asQueryableCall.Arguments[0], methodCallExpression.Arguments[1], out var arrayContainsTranslation))
        {
            return arrayContainsTranslation;
        }

        if (method.DeclaringType == typeof(Queryable)
            && (method.Name == nameof(Queryable.Any) || method.Name == nameof(Queryable.All))
            && methodCallExpression.Arguments.Count == 2
            && methodCallExpression.Arguments[0] is MethodCallExpression { Method.Name: nameof(Queryable.AsQueryable) } arrayAnyAllSource
            && TryTranslateArrayElementPredicate(
                arrayAnyAllSource.Arguments[0], methodCallExpression.Arguments[1], isAny: method.Name == nameof(Queryable.Any),
                out var arrayAnyAllTranslation))
        {
            return arrayAnyAllTranslation;
        }

        // Parameterless arrayProperty.Any()
        if (method.DeclaringType == typeof(Queryable)
            && method.Name == nameof(Queryable.Any)
            && methodCallExpression.Arguments.Count == 1
            && methodCallExpression.Arguments[0] is MethodCallExpression { Method.Name: nameof(Queryable.AsQueryable) } arrayAnySource
            && TryTranslateArrayAny(arrayAnySource.Arguments[0], out var arrayAnyTranslation))
        {
            return arrayAnyTranslation;
        }

        return base.VisitMethodCall(methodCallExpression);
    }

    private bool TryTranslateArrayAny(Expression collectionAccessExpression, [NotNullWhen(true)] out SqlExpression? translation)
    {
        if (Visit(collectionAccessExpression) is not SqlExpression arrayColumn)
        {
            translation = null;
            return false;
        }

        SqlExpression arraySize = _sqlExpressionFactory.Function(
            "ARRAY_SIZE",
            new[] { arrayColumn },
            nullable: true,
            argumentsPropagateNullability: Statics.TrueArrays[1],
            typeof(int));

        SqlExpression sizeGreaterThanZero = _sqlExpressionFactory.GreaterThan(arraySize, _sqlExpressionFactory.Constant(0));

        SqlExpression nvl = _sqlExpressionFactory.Function(
            "NVL",
            new[] { sizeGreaterThanZero, _sqlExpressionFactory.Constant(false) },
            nullable: true,
            argumentsPropagateNullability: Statics.TrueArrays[2],
            typeof(bool));

        translation = _sqlExpressionFactory.ApplyDefaultTypeMapping(nvl);
        return true;
    }

    private bool TryTranslateArrayElementPredicate(
        Expression collectionAccessExpression,
        Expression predicateExpression,
        bool isAny,
        [NotNullWhen(true)] out SqlExpression? translation)
    {
        translation = null;

        (Type? entityClrType, string? propertyName) = collectionAccessExpression switch
        {
            MethodCallExpression { Method.Name: nameof(EF.Property) } efPropertyCall
                when efPropertyCall.Arguments[1] is ConstantExpression { Value: string name }
                => (efPropertyCall.Arguments[0].Type, name),
            MemberExpression { Expression: not null } memberExpression
                => (memberExpression.Expression.Type, memberExpression.Member.Name),
            _ => (null, null)
        };

        if (entityClrType is null || propertyName is null)
        {
            return false;
        }

        IEntityType? entityType = _queryCompilationContext.Model.FindEntityType(entityClrType);
        IProperty? arrayProperty = entityType?.FindProperty(propertyName);
        IReadOnlyList<IProperty>? primaryKeyProperties = entityType?.FindPrimaryKey()?.Properties;

        // This translation strategy needs a single-column primary key to correlate the derived table back to the
        // entity being queried; composite keys and keyless entities aren't supported here.
        // TODO: support composite primary keys (group by / row-value IN on all key columns).
        if (entityType is null || arrayProperty is null || primaryKeyProperties is not { Count: 1 })
        {
            return false;
        }

        string? table = entityType.GetTableName();
        string? arrayColumn = arrayProperty.GetColumnName();
        string? pkColumn = primaryKeyProperties[0].GetColumnName();
        if (table is null || arrayColumn is null || pkColumn is null)
        {
            return false;
        }

        if (predicateExpression is not UnaryExpression { Operand: LambdaExpression lambda }
            || lambda.Body is not MethodCallExpression
            {
                Method: { Name: nameof(string.Contains) or nameof(string.StartsWith) or nameof(string.EndsWith) }
            } predicateCall
            || predicateCall.Object != lambda.Parameters[0]
            || predicateCall.Arguments.Count != 1)
        {
            return false;
        }

        StartsEndsWithContains predicateType = predicateCall.Method.Name switch
        {
            nameof(string.StartsWith) => StartsEndsWithContains.StartsWith,
            nameof(string.EndsWith) => StartsEndsWithContains.EndsWith,
            _ => StartsEndsWithContains.Contains
        };

        if (!TryBuildLikePattern(predicateCall.Arguments[0], predicateType, out SqlExpression? likePattern))
        {
            return false;
        }

        SqlExpression? arrayColumnForEmptyCheck = isAny ? null : Visit(collectionAccessExpression) as SqlExpression;
        if (!isAny && arrayColumnForEmptyCheck is null)
        {
            return false;
        }

        string quotedSchema = entityType.GetSchema() is { } schema
            ? $"{QuoteIdentifier(schema)}.{QuoteIdentifier(table)}"
            : QuoteIdentifier(table);
        string quotedArrayColumn = QuoteIdentifier(arrayColumn);
        string quotedPkColumn = QuoteIdentifier(pkColumn);

        const string outerAlias = "flt_src";
        const string flattenAlias = "flt_elem";

        SqlExpression elementValue = _sqlExpressionFactory.Fragment($"{flattenAlias}.\"VALUE\"::STRING");
        SqlExpression elementPredicate = _sqlExpressionFactory.Like(
            elementValue, likePattern, _sqlExpressionFactory.Constant(LikeEscapeCharString));

        // For All(predicate), an element fails the condition when the predicate is false; count those instead.
        if (!isAny)
        {
            elementPredicate = _sqlExpressionFactory.Not(elementPredicate);
        }

        SqlExpression countIf = _sqlExpressionFactory.Function(
            "COUNT_IF",
            new[] { elementPredicate },
            nullable: true,
            argumentsPropagateNullability: Statics.TrueArrays[1],
            typeof(int));

        SqlExpression havingCondition = isAny
            ? _sqlExpressionFactory.NotEqual(countIf, _sqlExpressionFactory.Constant(0))
            : _sqlExpressionFactory.Equal(countIf, _sqlExpressionFactory.Constant(0));

        string subquerySqlPrefix =
            $"SELECT {outerAlias}.{quotedPkColumn} " +
            $"FROM {quotedSchema} AS {outerAlias}, LATERAL FLATTEN(INPUT => {outerAlias}.{quotedArrayColumn}) AS {flattenAlias} " +
            $"GROUP BY {outerAlias}.{quotedPkColumn} HAVING";

        SqlExpression subquery = _sqlExpressionFactory.Function(
            subquerySqlPrefix,
            new[] { havingCondition },
            nullable: true,
            argumentsPropagateNullability: Statics.TrueArrays[1],
            typeof(bool));

        SqlExpression inExpression = _sqlExpressionFactory.Function(
            $"{quotedPkColumn} IN",
            new[] { subquery },
            nullable: true,
            argumentsPropagateNullability: Statics.TrueArrays[1],
            typeof(bool));

        // Guard against NULL from the IN check (e.g. no rows at all) so this always evaluates to a proper boolean.
        SqlExpression nvl = _sqlExpressionFactory.Function(
            "NVL",
            new[] { inExpression, _sqlExpressionFactory.Constant(false) },
            nullable: true,
            argumentsPropagateNullability: Statics.TrueArrays[2],
            typeof(bool));

        SqlExpression result = nvl;

        if (!isAny && arrayColumnForEmptyCheck is not null)
        {
            SqlExpression arraySize = _sqlExpressionFactory.Function(
                "ARRAY_SIZE",
                new[] { arrayColumnForEmptyCheck },
                nullable: true,
                argumentsPropagateNullability: Statics.TrueArrays[1],
                typeof(int));

            SqlExpression arrayIsEmptyOrNull = _sqlExpressionFactory.OrElse(
                _sqlExpressionFactory.IsNull(arrayColumnForEmptyCheck),
                _sqlExpressionFactory.Equal(arraySize, _sqlExpressionFactory.Constant(0)));

            result = _sqlExpressionFactory.OrElse(arrayIsEmptyOrNull, nvl);
        }

        translation = _sqlExpressionFactory.ApplyDefaultTypeMapping(result);
        return true;
    }

    private bool TryBuildLikePattern(
        Expression patternValueExpression,
        StartsEndsWithContains methodType,
        [NotNullWhen(true)] out SqlExpression? likePattern)
    {
        if (Visit(patternValueExpression) is not SqlExpression visited)
        {
            likePattern = null;
            return false;
        }

        switch (visited)
        {
            case SqlConstantExpression { Value: string s }:
                likePattern = _sqlExpressionFactory.ApplyDefaultTypeMapping(
                    _sqlExpressionFactory.Constant(BuildLikePatternText(s, methodType)));
                return true;

            case SqlParameterExpression patternParameter:
            {
                var lambda = Expression.Lambda(
                    Expression.Call(
                        EscapeLikePatternParameterMethod,
                        QueryCompilationContext.QueryContextParameter,
                        Expression.Constant(patternParameter.Name),
                        Expression.Constant(methodType)),
                    QueryCompilationContext.QueryContextParameter);

                var escapedPatternParameter =
                    _queryCompilationContext.RegisterRuntimeParameter(
                        $"{patternParameter.Name}_{methodType.ToString().ToLower(CultureInfo.InvariantCulture)}", lambda);

                likePattern = new SqlParameterExpression(
                    escapedPatternParameter.Name!, escapedPatternParameter.Type, Dependencies.TypeMappingSource.FindMapping(typeof(string)));
                return true;
            }

            default:
                likePattern = null;
                return false;
        }
    }

    private static string BuildLikePatternText(string pattern, StartsEndsWithContains methodType)
        => methodType switch
        {
            StartsEndsWithContains.StartsWith => EscapeLikePattern(pattern) + '%',
            StartsEndsWithContains.EndsWith => '%' + EscapeLikePattern(pattern),
            StartsEndsWithContains.Contains => $"%{EscapeLikePattern(pattern)}%",
            _ => throw new ArgumentOutOfRangeException(nameof(methodType), methodType, null)
        };

    private static string QuoteIdentifier(string name) => $"\"{name.Replace("\"", "\"\"")}\"";

    private bool TryTranslateArrayContains(
        Expression collectionExpression,
        Expression valueExpression,
        [NotNullWhen(true)] out SqlExpression? translation)
    {
        if (Visit(collectionExpression) is SqlExpression collection
            && Visit(valueExpression) is SqlExpression value)
        {
            value = _sqlExpressionFactory.ApplyDefaultTypeMapping(value);

            // Array elements are stored as VARIANT, so the searched value must be cast to VARIANT to compare equal.
            SqlExpression valueAsVariant = _sqlExpressionFactory.Function(
                "TO_VARIANT",
                new[] { value },
                nullable: true,
                argumentsPropagateNullability: Statics.TrueArrays[1],
                typeof(object),
                SnowflakeVariantTypeMapping.Default);

            SqlExpression arrayContains = _sqlExpressionFactory.Function(
                "ARRAY_CONTAINS",
                new[] { valueAsVariant, collection },
                nullable: true,
                argumentsPropagateNullability: Statics.TrueArrays[2],
                returnType: typeof(bool));

            translation = _sqlExpressionFactory.ApplyDefaultTypeMapping(arrayContains);
            return true;
        }

        translation = null;
        return false;
    }

    private Expression TranslateByteArrayElementAccess(Expression array, Expression index, Type resultType)
    {
        var visitedArray = Visit(array);
        var visitedIndex = Visit(index);

        return visitedArray is SqlExpression sqlArray
               && visitedIndex is SqlExpression sqlIndex
            ? Dependencies.SqlExpressionFactory.Convert(
                Dependencies.SqlExpressionFactory.Function(
                    "SUBSTRING",
                    new[]
                    {
                        sqlArray,
                        Dependencies.SqlExpressionFactory.Add(
                            Dependencies.SqlExpressionFactory.ApplyDefaultTypeMapping(sqlIndex),
                            Dependencies.SqlExpressionFactory.Constant(1)),
                        Dependencies.SqlExpressionFactory.Constant(1)
                    },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true, true, true },
                    typeof(byte[])),
                resultType)
            : QueryCompilationContext.NotTranslatedExpression;
    }

    protected override Expression VisitUnary(UnaryExpression unaryExpression)
    {
        if (unaryExpression.NodeType == ExpressionType.ArrayLength
            && unaryExpression.Operand.Type == typeof(byte[]))
        {
            if (!(base.Visit(unaryExpression.Operand) is SqlExpression sqlExpression))
            {
                return QueryCompilationContext.NotTranslatedExpression;
            }

            //var isBinaryMaxDataType = GetProviderType(sqlExpression) == "varbinary(max)" || sqlExpression is SqlParameterExpression;
            var dataLengthSqlFunction = Dependencies.SqlExpressionFactory.Function(
                "LENGTH",
                new[] { sqlExpression },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(int));

            return dataLengthSqlFunction;
        }

        return base.VisitUnary(unaryExpression);
    }

    #region StartsWith/EndsWith/Contains

    private bool TryTranslateStartsEndsWithContains(
        Expression instance,
        Expression pattern,
        StartsEndsWithContains methodType,
        [NotNullWhen(true)] out SqlExpression? translation)
    {
        if (Visit(instance) is not SqlExpression translatedInstance
            || Visit(pattern) is not SqlExpression translatedPattern)
        {
            translation = null;
            return false;
        }

        var stringTypeMapping = ExpressionExtensions.InferTypeMapping(translatedInstance, translatedPattern);

        translatedInstance = _sqlExpressionFactory.ApplyTypeMapping(translatedInstance, stringTypeMapping);
        translatedPattern = _sqlExpressionFactory.ApplyTypeMapping(translatedPattern, stringTypeMapping);

        switch (translatedPattern)
        {
            case SqlConstantExpression patternConstant:
                {
                    // The pattern is constant. Aside from null and empty string, we escape all special characters (%, _, \) and send a
                    // simple LIKE
                    translation = patternConstant.Value switch
                    {
                        null => _sqlExpressionFactory.Like(translatedInstance, _sqlExpressionFactory.Constant(null, typeof(string), stringTypeMapping)),

                        // In .NET, all strings start with/end with/contain the empty string, but SQL LIKE return false for empty patterns.
                        // Return % which always matches instead.
                        // Note that we don't just return a true constant, since null strings shouldn't match even an empty string
                        // (but SqlNullabilityProcess will convert this to a true constant if the instance is non-nullable)
                        "" => _sqlExpressionFactory.Like(translatedInstance, _sqlExpressionFactory.Constant("%")),

                        string s => _sqlExpressionFactory.Like(
                            translatedInstance,
                            _sqlExpressionFactory.Constant(
                                methodType switch
                                {
                                    StartsEndsWithContains.StartsWith => EscapeLikePattern(s) + '%',
                                    StartsEndsWithContains.EndsWith => '%' + EscapeLikePattern(s),
                                    StartsEndsWithContains.Contains => $"%{EscapeLikePattern(s)}%",

                                    _ => throw new ArgumentOutOfRangeException(nameof(methodType), methodType, null)
                                }),
                            _sqlExpressionFactory.Constant(LikeEscapeCharString)),

                        _ => throw new UnreachableException()
                    };

                    return true;
                }

            case SqlParameterExpression patternParameter:
                {
                    // The pattern is a parameter, register a runtime parameter that will contain the rewritten LIKE pattern, where
                    // all special characters have been escaped.
                    var lambda = Expression.Lambda(
                        Expression.Call(
                            EscapeLikePatternParameterMethod,
                            QueryCompilationContext.QueryContextParameter,
                            Expression.Constant(patternParameter.Name),
                            Expression.Constant(methodType)),
                        QueryCompilationContext.QueryContextParameter);

                    var escapedPatternParameter =
                        _queryCompilationContext.RegisterRuntimeParameter(
                            $"{patternParameter.Name}_{methodType.ToString().ToLower(CultureInfo.InvariantCulture)}", lambda);

                    translation = _sqlExpressionFactory.Like(
                        translatedInstance,
                        new SqlParameterExpression(escapedPatternParameter.Name!, escapedPatternParameter.Type, stringTypeMapping),
                        _sqlExpressionFactory.Constant(LikeEscapeCharString));

                    return true;
                }

            default:
                // The pattern is a column or a complex expression; the possible special characters in the pattern cannot be escaped,
                // preventing us from translating to LIKE.
                switch (methodType)
                {
                    // For StartsWith/EndsWith, use LEFT or RIGHT instead to extract substring and compare:
                    // WHERE instance IS NOT NULL AND pattern IS NOT NULL AND LEFT(instance, LEN(pattern)) = pattern
                    // This is less efficient than LIKE (i.e. StartsWith does an index scan instead of seek), but we have no choice.
                    case StartsEndsWithContains.StartsWith or StartsEndsWithContains.EndsWith:
                        translation =
                            _sqlExpressionFactory.Function(
                                methodType is StartsEndsWithContains.StartsWith ? "LEFT" : "RIGHT",
                                new[]
                                {
                                    translatedInstance,
                                    _sqlExpressionFactory.Function(
                                        "LENGTH", new[] { translatedPattern }, nullable: true,
                                        argumentsPropagateNullability: new[] { true }, typeof(int))
                                }, nullable: true, argumentsPropagateNullability: new[] { true, true }, typeof(string),
                                stringTypeMapping);

                        // LEFT/RIGHT of a citext return a text, so for non-default text mappings we apply an explicit cast.
                        if (translatedInstance.TypeMapping is { StoreType: not "text" })
                        {
                            translation = _sqlExpressionFactory.Convert(translation, typeof(string), translatedInstance.TypeMapping);
                        }

                        // We compensate for the case where both the instance and the pattern are null (null.StartsWith(null)); a simple
                        // equality would yield true in that case, but we want false.
                        translation =
                            _sqlExpressionFactory.AndAlso(
                                _sqlExpressionFactory.IsNotNull(translatedInstance),
                                _sqlExpressionFactory.AndAlso(
                                    _sqlExpressionFactory.IsNotNull(translatedPattern),
                                    _sqlExpressionFactory.Equal(translation, translatedPattern)));

                        break;

                    // For Contains, just use strpos and check if the result is greater than 0. Note that strpos returns 1 when the pattern
                    // is an empty string, just like .NET Contains (so no need to compensate)
                    case StartsEndsWithContains.Contains:
                        translation =
                            _sqlExpressionFactory.AndAlso(
                                _sqlExpressionFactory.IsNotNull(translatedInstance),
                                _sqlExpressionFactory.AndAlso(
                                    _sqlExpressionFactory.IsNotNull(translatedPattern),
                                    _sqlExpressionFactory.GreaterThan(
                                        _sqlExpressionFactory.Function(
                                            "POSITION", new[] { translatedPattern, translatedInstance }, nullable: true,
                                            argumentsPropagateNullability: new[] { true, true }, typeof(int)),
                                        _sqlExpressionFactory.Constant(0))));
                        break;

                    default:
                        throw new UnreachableException();
                }

                return true;
        }
    }

    private static string? ConstructLikePatternParameter(
        QueryContext queryContext,
        string baseParameterName,
        StartsEndsWithContains methodType)
        => queryContext.Parameters[baseParameterName] switch
        {
            null => null,

            // In .NET, all strings start/end with the empty string, but SQL LIKE return false for empty patterns.
            // Return % which always matches instead.
            "" => "%",

            string s => methodType switch
            {
                StartsEndsWithContains.StartsWith => EscapeLikePattern(s) + '%',
                StartsEndsWithContains.EndsWith => '%' + EscapeLikePattern(s),
                StartsEndsWithContains.Contains => $"%{EscapeLikePattern(s)}%",
                _ => throw new ArgumentOutOfRangeException(nameof(methodType), methodType, null)
            },

            _ => throw new UnreachableException()
        };

    private enum StartsEndsWithContains
    {
        StartsWith,
        EndsWith,
        Contains
    }

    private static bool IsLikeWildChar(char c)
        => c is '%' or '_';

    private static string EscapeLikePattern(string pattern)
    {
        var builder = new StringBuilder();
        for (var i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            if (IsLikeWildChar(c) || c == LikeEscapeChar)
            {
                builder.Append(LikeEscapeChar);
            }

            builder.Append(c);
        }

        return builder.ToString();
    }

    #endregion StartsWith/EndsWith/Contains
}

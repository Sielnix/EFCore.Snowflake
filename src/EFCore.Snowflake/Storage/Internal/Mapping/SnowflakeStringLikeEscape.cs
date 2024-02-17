using System.Text;

namespace EFCore.Snowflake.Storage.Internal.Mapping;
public static class SnowflakeStringLikeEscape
{
    private static readonly HashSet<char> CharactersToEscape =
    [
        '\'',
        '"',
        '\\',
        '\b',
        '\f',
        '\n',
        '\r',
        '\t',
        '\0'
    ];

    public static string GenerateSqlLiteral(char literal)
    {
        if (!CharactersToEscape.Contains(literal))
        {
            return $"'{literal}'";
        }

        return $"'\\{literal}'";
    }
    
    public static string EscapeSqlLiteral(string literal)
    {
        int charsToEscape = literal.Count(static c => CharactersToEscape.Contains(c));
        if (charsToEscape == 0)
        {
            return literal;
        }

        StringBuilder sb = new(literal.Length + charsToEscape);
        foreach (var character in literal)
        {
            if (CharactersToEscape.Contains(character))
            {
                sb.Append('\\');
            }

            sb.Append(character);
        }

        return sb.ToString();
    }
}

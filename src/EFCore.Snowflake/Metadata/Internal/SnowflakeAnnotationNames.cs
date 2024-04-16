namespace EFCore.Snowflake.Metadata.Internal;
internal static class SnowflakeAnnotationNames
{
    public const string Prefix = "Snowflake:";

    public const string ValueGenerationStrategy = Prefix + "ValueGenerationStrategy";
    public const string Identity = Prefix + "Identity";

    public const string SequenceName = Prefix + "SequenceName";
    public const string SequenceSchema = Prefix + "SequenceSchema";

    public const string IdentityIncrement = Prefix + "IdentityIncrement";
    public const string IdentitySeed = Prefix + "IdentitySeed";

    public const string IndexBehavior = Prefix + "IndexBehavior";
}

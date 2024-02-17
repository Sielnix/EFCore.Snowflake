using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Internal;

namespace EFCore.Snowflake.Storage.Internal;

internal class SnowflakeTypeMappedRelationalParameter : TypeMappedRelationalParameter
{
    private readonly RelationalTypeMapping _relationalTypeMapping;
    private readonly bool? _nullable;

    public SnowflakeTypeMappedRelationalParameter(TypeMappedRelationalParameter sourceTypeMapped)
        : this(
            invariantName: sourceTypeMapped.InvariantName,
            name: sourceTypeMapped.Name,
            relationalTypeMapping: sourceTypeMapped.GetTypeMapping(),
            nullable: sourceTypeMapped.GetIsNullable(),
            direction: sourceTypeMapped.Direction)
    {
    }

    private SnowflakeTypeMappedRelationalParameter(
        string invariantName,
        string name,
        RelationalTypeMapping relationalTypeMapping,
        bool? nullable,
        ParameterDirection direction)
        : base(
            invariantName: invariantName,
            name: name,
            relationalTypeMapping: relationalTypeMapping,
            nullable: nullable,
            direction: direction)
    {
        _relationalTypeMapping = relationalTypeMapping;
        _nullable = nullable;
    }

    public override void AddDbParameter(DbCommand command, object? value)
    {
        command.Parameters.Add(
            _relationalTypeMapping.CreateParameter(command, InvariantName, value, _nullable, Direction));
    }
}

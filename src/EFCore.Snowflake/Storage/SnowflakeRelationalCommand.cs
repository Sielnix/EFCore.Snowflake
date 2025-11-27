using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage;

public class SnowflakeRelationalCommand : RelationalCommand
{
    public SnowflakeRelationalCommand(RelationalCommandBuilderDependencies dependencies, string commandText, string logCommandText, IReadOnlyList<IRelationalParameter> parameters)
        : base(dependencies, commandText, logCommandText, parameters)
    {
    }
    
    public override DbCommand CreateDbCommand(
        RelationalCommandParameterObject parameterObject,
        Guid commandId,
        DbCommandMethod commandMethod)
    {
        DbCommand command = base.CreateDbCommand(parameterObject, commandId, commandMethod);

        foreach (DbParameter commandParameter in command.Parameters)
        {
            string name = commandParameter.ParameterName;

            if (name.StartsWith(':'))
            {
                commandParameter.ParameterName = name.Substring(1);
            }
        }

        return command;
    }
}

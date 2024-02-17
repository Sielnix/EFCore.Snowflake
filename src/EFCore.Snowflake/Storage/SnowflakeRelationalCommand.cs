using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Snowflake.Storage;
internal class SnowflakeRelationalCommand : RelationalCommand
{
    private IReadOnlyList<IRelationalParameter>? lastBaseParameters;
    private IReadOnlyList<IRelationalParameter>? overridenParameters;

    public SnowflakeRelationalCommand(RelationalCommandBuilderDependencies dependencies, string commandText, IReadOnlyList<IRelationalParameter> parameters)
        : base(dependencies, commandText, parameters)
    {
    }

    public override IReadOnlyList<IRelationalParameter> Parameters
    {
        get
        {
            IReadOnlyList<IRelationalParameter> baseParameters = base.Parameters;
            if (overridenParameters is null || !ReferenceEquals(baseParameters, lastBaseParameters))
            {
                overridenParameters = WrapTypeMappedRelationalParameters(baseParameters);
                lastBaseParameters = baseParameters;
            }

            return overridenParameters;
        }
    }

    public override DbCommand CreateDbCommand(
        RelationalCommandParameterObject parameterObject,
        Guid commandId,
        DbCommandMethod commandMethod)
    {
        DbCommand command = base.CreateDbCommand(parameterObject, commandId, commandMethod);

        //int i = 1;
        //foreach (DbParameter commandParameter in command.Parameters)
        //{
        //    commandParameter.ParameterName = i.ToString(CultureInfo.InvariantCulture);
        //    i++;
        //}

        foreach (DbParameter commandParameter in command.Parameters)
        {
            string name = commandParameter.ParameterName;
            //if (!name.StartsWith(':'))
            //{
            //    throw new InvalidOperationException($"Parameter {name} doesn't start with ':'");
            //}

            //commandParameter.ParameterName = name.Substring(1);
            //if (name.Contains("1"))
            //{
            //    ((SnowflakeDbParameter)commandParameter).SFDataType = SFDataType.TEXT;
            //}

            if (name.StartsWith(':'))
            {
                commandParameter.ParameterName = name.Substring(1);
            }
        }

        return command;
    }

    private static IReadOnlyList<IRelationalParameter> WrapTypeMappedRelationalParameters(
        IReadOnlyList<IRelationalParameter> parameters)
    {
        //if (parameters.All(p => p is not TypeMappedRelationalParameter))
        //{
        //    return parameters;
        //}

        //int count = parameters.Count;
        //IRelationalParameter[] mappedList = new IRelationalParameter[count];
        //for (int i = 0; i < count; i++)
        //{
        //    IRelationalParameter sourceParam = parameters[i];
        //    if (sourceParam is TypeMappedRelationalParameter typeMapped)
        //    {
        //        mappedList[i] = new SnowflakeTypeMappedRelationalParameter(typeMapped);
        //    }
        //    else
        //    {
        //        mappedList[i] = sourceParam;
        //    }
        //}

        //return mappedList;
        return parameters;
    }
}

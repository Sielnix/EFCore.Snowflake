## About

_EntityFrameworkCore.Snowflake_ is the Entity Framework Core (EF Core) provider for [Snowflake](https://www.snowflake.com).

It is build on top of [Snowflake.Data](https://github.com/snowflakedb/snowflake-connector-net).

## How to Use

```csharp
public class Startup
{
    public void ConfigureServices(IServiceCollection services)
    {
        // Replace with your connection string.
        var connectionString = "account=YOUR_ACCOUNT;host=UR_HOST.us-east-1.snowflakecomputing.com;user=UR_USER;password=UR_PASSWORD;db=TESTDB;schema=PUBLIC;warehouse=UR_WAREHOUSE";

        // Replace 'YourDbContext' with the name of your own DbContext derived class.
        services.AddDbContext<YourDbContext>(
            dbContextOptions => dbContextOptions
                .UseSnowflake(connectionString)
        );
    }
}
```

## Feedback

**Feel free to submit any feedback - bug reports or feature requests**. All feedback is welcome at [GitHub repository](https://github.com/Sielnix/EFCore.Snowflake).


## Key Features

* Query support
* Auto increment
* Scaffolding support
* Database versioning support

## Known issues

* Spatial types not supported
* Variant type inserts data with json escaping (bug in Snowflake.Data connector)
* Database name is required to be named UPPERCASE (bug in Snowflake.Data)
* More advanced queries, such as LEFT LATERAL JOIN or more complex subqueries fails, because they are not supported by Snowflake right now

## Related Packages

* Other Packages
  * Snowflake ADO.NET connetor [Snowflake.Data](https://github.com/snowflakedb/snowflake-connector-net).
  * [Microsoft.EntityFrameworkCore](https://www.nuget.org/packages/Microsoft.EntityFrameworkCore)

## License

_EntityFrameworkCore.Snowflake_ is released as open source under the [GNU Lesser General Public License v3.0 only](https://github.com/Sielnix/EFCore.Snowflake/blob/main/LICENSE).

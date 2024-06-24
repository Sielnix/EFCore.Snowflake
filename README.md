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

## Scaffolding

If you wish to create model from existing Snowflake database, then follow [this](https://learn.microsoft.com/en-us/ef/core/cli/dotnet#dotnet-ef-dbcontext-scaffold) steps. Provider name is `EFCore.Snowflake`.  Ensure you have ef tools installed (`dotnet tool install --global dotnet-ef`).

## Database generation

Please be aware, that Snowflake default naming convention is UPPERCASE, while C# default naming convention is PascalCase. In order to follow Snowflake's uppercase convention when generating new database with code-first approach you have to map each table and column to database name with uppercase. Use `.ToTable("TABLE_NAME")` and `entity.Property(e => e.Id).HasColumnName("ID")` mapping methods.

## Type mapping

- All basic C# types are supported.
- Spatial types are not supported
- When scaffolding Variant, Array or Object column type - it is mapped as C# `string` type with json data
- Snowflake Array column can be mapped to C# arrays. Example:
```csharp
public class SampleModel
{
    public long Id { get; set; }
    public string[]? ArrayColumn { get; set; }
}

public class SnowflakeDbContext : DbContext
{
    public DbSet<SampleModel> Models { get; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseSnowflake();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("PUBLIC");

        modelBuilder.Entity<SampleModel>(entity =>
        {
            entity.ToTable("SAMPLE_MODEL_", "PUBLIC");

            entity.HasKey(e => e.Id);

            entity.Property(e => e.Id)
                .HasColumnType("NUMBER(38,0)")
                .HasColumnName("ID");

            entity.Property(e => e.ArrayColumn)
                .HasColumnType("ARRAY")
                .HasColumnName("ARRAY_COLUMN");;
        });
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
* Sequences support, along with primary key
* Transient tables scaffolding and generation - use `entity.ToTable("TABLE_NAME", t => t.IsTransient())`

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

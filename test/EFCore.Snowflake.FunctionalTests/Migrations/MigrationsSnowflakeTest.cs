using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Snowflake.Data.Client;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests.Migrations;

public class MigrationsSnowflakeTest : MigrationsTestBase<MigrationsSnowflakeTest.MigrationsSnowflakeFixture>
{
    public MigrationsSnowflakeTest(MigrationsSnowflakeFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    protected override string NonDefaultCollation => "de-ci-pi";

    public override async Task Add_check_constraint_with_name()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Add_check_constraint_with_name());
    }

    public override async Task Alter_check_constraint()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_check_constraint());
    }


    public override async Task Drop_check_constraint()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Drop_check_constraint());
    }

    public override async Task Add_column_with_check_constraint()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Add_column_with_check_constraint());
    }

    public override async Task Add_column_computed_with_collation(bool stored)
    {
        if (stored)
        {
            await Assert.ThrowsAsync<NotSupportedException>(() => base.Create_table_with_computed_column(stored));
            return;
        }

        await base.Add_column_computed_with_collation(stored);
    }

    public override async Task Add_column_with_computedSql(bool? stored)
    {
        Task MethodImpl(bool? stored)
        => Test(
            builder => builder.Entity(
                "People", e =>
                {
                    e.Property<int>("Id");
                    e.Property<int>("X");
                    e.Property<int>("Y");
                }),
            builder => { },
            // ONLY CHANGE: property type to int, since otherwise snowflake requires manual cast from int to varchar in computed sql
            builder => builder.Entity("People").Property<int>("Sum")
                .HasComputedColumnSql($"{DelimitIdentifier("X")} + {DelimitIdentifier("Y")}", stored),
            model =>
            {
                var table = Assert.Single(model.Tables);
                var sumColumn = Assert.Single(table.Columns, c => c.Name == "Sum");
                if (AssertComputedColumns)
                {
                    Assert.Contains("X", sumColumn.ComputedColumnSql);
                    Assert.Contains("Y", sumColumn.ComputedColumnSql);
                    if (stored != null)
                    {
                        Assert.Equal(stored, sumColumn.IsStored);
                    }
                }
            });

        if (stored == true)
        {
            // Stored generated columns aren't supported
            await Assert.ThrowsAsync<NotSupportedException>(() => MethodImpl(stored));
            return;
        }

        await MethodImpl(stored);
    }

    [Fact(Skip = "SNOWFLAKE BUG 00681145")]
    public override Task Add_column_with_defaultValue_datetime()
    {
        return base.Add_column_with_defaultValue_datetime();
    }

    public override Task Add_column_with_defaultValueSql()
    {
        // SQL expression not supported by Snowflake
        return Task.CompletedTask;
    }

    public override Task Add_foreign_key()
        => Test(
            builder =>
            {
                builder.Entity(
                    "Customers", e =>
                    {
                        e.Property<int>("Id");
                        e.HasKey("Id");
                    });
                builder.Entity(
                    "Orders", e =>
                    {
                        e.Property<int>("Id");
                        e.Property<int>("CustomerId");
                    });
            },
            builder => { },
            builder => builder.Entity("Orders").HasOne("Customers").WithMany()
                .HasForeignKey("CustomerId"),
            model =>
            {
                var customersTable = Assert.Single(model.Tables, t => t.Name == "Customers");
                var ordersTable = Assert.Single(model.Tables, t => t.Name == "Orders");
                var foreignKey = ordersTable.ForeignKeys.Single();
                if (AssertConstraintNames)
                {
                    Assert.Equal("FK_Orders_Customers_CustomerId", foreignKey.Name);
                }

                // ONLY CHANGE - Expected NO ACTION
                Assert.Equal(ReferentialAction.NoAction, foreignKey.OnDelete);
                Assert.Same(customersTable, foreignKey.PrincipalTable);
                Assert.Same(customersTable.Columns.Single(), Assert.Single(foreignKey.PrincipalColumns));
                Assert.Equal("CustomerId", Assert.Single(foreignKey.Columns).Name);
            });

    public override Task Alter_column_change_computed()
        => Test(
            builder => builder.Entity(
                "People", e =>
                {
                    // ONLY CHANGE - Added column ordering
                    e.Property<int>("Id").HasColumnOrder(0);
                    e.Property<int>("X").HasColumnOrder(1);
                    e.Property<int>("Y").HasColumnOrder(2);
                    e.Property<int>("Sum").HasColumnOrder(3);
                }),
            builder => builder.Entity("People").Property<int>("Sum")
                .HasComputedColumnSql($"{DelimitIdentifier("X")} + {DelimitIdentifier("Y")}"),
            builder => builder.Entity("People").Property<int>("Sum")
                .HasComputedColumnSql($"{DelimitIdentifier("X")} - {DelimitIdentifier("Y")}"),
            model =>
            {
                var table = Assert.Single(model.Tables);
                var sumColumn = Assert.Single(table.Columns, c => c.Name == "Sum");
                if (AssertComputedColumns)
                {
                    Assert.Contains("X", sumColumn.ComputedColumnSql);
                    Assert.Contains("Y", sumColumn.ComputedColumnSql);
                    Assert.Contains("-", sumColumn.ComputedColumnSql);
                }
            });

    public override async Task Alter_column_change_computed_recreates_indexes()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_change_computed_recreates_indexes());
    }

    public override async Task Alter_column_change_computed_type()
    {
        Task MethodImpl()
            => Test(
            builder => builder.Entity(
                "People", e =>
                {
                    // ONLY CHANGE - added column orders
                    e.Property<int>("Id").HasColumnOrder(0);
                    e.Property<int>("X").HasColumnOrder(1);
                    e.Property<int>("Y").HasColumnOrder(2);
                    e.Property<int>("Sum").HasColumnOrder(3);
                }),
            builder => builder.Entity("People").Property<int>("Sum")
                .HasComputedColumnSql($"{DelimitIdentifier("X")} + {DelimitIdentifier("Y")}", stored: false),
            builder => builder.Entity("People").Property<int>("Sum")
                .HasComputedColumnSql($"{DelimitIdentifier("X")} + {DelimitIdentifier("Y")}", stored: true),
            model =>
            {
                var table = Assert.Single(model.Tables);
                var sumColumn = Assert.Single(table.Columns, c => c.Name == "Sum");
                if (AssertComputedColumns)
                {
                    Assert.True(sumColumn.IsStored);
                }
            });

        await Assert.ThrowsAsync<NotSupportedException>(async () => await MethodImpl());
    }

    public override async Task Alter_column_make_computed(bool? stored)
    {
        if (stored == true)
        {
            await Assert.ThrowsAsync<NotSupportedException>(() => base.Add_column_with_computedSql(stored));
            return;
        }

        await base.Alter_column_make_computed(stored);
    }

    public override Task Alter_column_make_non_computed()
        => Test(
            builder => builder.Entity(
                "People", e =>
                {
                    // ONLY CHANGE - added column orders
                    e.Property<int>("Id").HasColumnOrder(0);
                    e.Property<int>("X").HasColumnOrder(1);
                    e.Property<int>("Y").HasColumnOrder(2);
                }),
            builder => builder.Entity("People").Property<int>("Sum")
                .HasComputedColumnSql($"{DelimitIdentifier("X")} + {DelimitIdentifier("Y")}")
                .HasColumnOrder(3),
            builder => builder.Entity("People").Property<int>("Sum"),
            model =>
            {
                var table = Assert.Single(model.Tables);
                var sumColumn = Assert.Single(table.Columns, c => c.Name == "Sum");
                Assert.Null(sumColumn.ComputedColumnSql);
                Assert.NotEqual(true, sumColumn.IsStored);
            });

    public override async Task Alter_column_make_required_with_composite_index()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_make_required_with_composite_index());
    }

    public override async Task Alter_column_make_required_with_index()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_make_required_with_index());
    }

    public override async Task Alter_column_reset_collation()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_reset_collation());
    }

    public override async Task Alter_column_set_collation()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_set_collation());
    }

    public override async Task Alter_index_change_sort_order()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_index_change_sort_order());
    }

    public override async Task Alter_index_make_unique()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_index_make_unique());
    }
    
    public override async Task Create_index()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Create_index());
    }

    public override async Task Create_index_descending()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Create_index_descending());
    }

    public override async Task Create_index_descending_mixed()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Create_index_descending_mixed());
    }

    public override async Task Create_index_unique()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Create_index_unique());
    }

    public override async Task Create_index_with_filter()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Create_index_with_filter());
    }

    public override async Task Create_unique_index_with_filter()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Create_unique_index_with_filter());
    }

    public override async Task Drop_index()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Drop_index());
    }

    public override async Task Rename_index()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Rename_index());
    }

    public override async Task Alter_sequence_all_settings()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_sequence_all_settings());
    }

    public override async Task Alter_sequence_restart_with()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_sequence_restart_with());
    }

    public override Task Create_sequence_all_settings()
        => Test(
            builder => { },
            builder => builder.HasSequence<long>("TestSequence", "dbo2")
                .StartsAt(3)
                .IncrementsBy(2)
                .HasMin(2)
                .HasMax(916)
                .IsCyclic(),
            model =>
            {
                var sequence = Assert.Single(model.Sequences);
                Assert.Equal("TestSequence", sequence.Name);
                Assert.Equal("dbo2", sequence.Schema);
                Assert.Equal(3, sequence.StartValue);
                Assert.Equal(2, sequence.IncrementBy);
                // ONLY CHANGE removed min,max, cyclic checks
            });

    public override async Task Create_table_all_settings()
    {
        var intStoreType = TypeMappingSource.FindMapping(typeof(int))!.StoreType;
        var char11StoreType = TypeMappingSource.FindMapping(typeof(string), storeTypeName: null, size: 11)!.StoreType;

        await Test(
            builder => builder.Entity(
                "Employers", e =>
                {
                    e.Property<int>("Id");
                    e.HasKey("Id");
                }),
            builder => { },
            builder => builder.Entity(
                "People", e =>
                {
                    e.ToTable(
                        "People", "dbo2", tb =>
                        {
                            // ONLY CHANGE - removed check constraint
                            tb.HasComment("Table comment");
                        });

                    e.Property<int>("CustomId");
                    e.Property<int>("EmployerId")
                        .HasComment("Employer ID comment");
                    e.Property<string>("SSN")
                        .HasColumnType(char11StoreType)
                        .UseCollation(NonDefaultCollation)
                        .IsRequired(false);

                    e.HasKey("CustomId");
                    e.HasAlternateKey("SSN");
                    e.HasOne("Employers").WithMany("People").HasForeignKey("EmployerId");
                }),
            model =>
            {
                var employersTable = Assert.Single(model.Tables, t => t.Name == "Employers");
                var peopleTable = Assert.Single(model.Tables, t => t.Name == "People");

                Assert.Equal("People", peopleTable.Name);
                if (AssertSchemaNames)
                {
                    Assert.Equal("dbo2", peopleTable.Schema);
                }

                Assert.Collection(
                    peopleTable.Columns.OrderBy(c => c.Name),
                    c =>
                    {
                        Assert.Equal("CustomId", c.Name);
                        Assert.False(c.IsNullable);
                        Assert.Equal(intStoreType, c.StoreType);
                        Assert.Null(c.Comment);
                    },
                    c =>
                    {
                        Assert.Equal("EmployerId", c.Name);
                        Assert.False(c.IsNullable);
                        Assert.Equal(intStoreType, c.StoreType);
                        if (AssertComments)
                        {
                            Assert.Equal("Employer ID comment", c.Comment);
                        }
                    },
                    c =>
                    {
                        Assert.Equal("SSN", c.Name);
                        Assert.False(c.IsNullable);
                        Assert.Equal(char11StoreType, c.StoreType);
                        Assert.Null(c.Comment);
                    });

                Assert.Same(
                    peopleTable.Columns.Single(c => c.Name == "CustomId"),
                    Assert.Single(peopleTable.PrimaryKey!.Columns));
                Assert.Same(
                    peopleTable.Columns.Single(c => c.Name == "SSN"),
                    Assert.Single(Assert.Single(peopleTable.UniqueConstraints).Columns));
                // TODO: Need to scaffold check constraints, https://github.com/aspnet/EntityFrameworkCore/issues/15408

                var foreignKey = Assert.Single(peopleTable.ForeignKeys);
                Assert.Same(peopleTable, foreignKey.Table);
                Assert.Same(peopleTable.Columns.Single(c => c.Name == "EmployerId"), Assert.Single(foreignKey.Columns));
                Assert.Same(employersTable, foreignKey.PrincipalTable);
                Assert.Same(employersTable.Columns.Single(), Assert.Single(foreignKey.PrincipalColumns));

                if (AssertComments)
                {
                    Assert.Equal("Table comment", peopleTable.Comment);
                }
            });
    }

    public override async Task Create_table_with_computed_column(bool? stored)
    {
        Task TestImpl()
            => Test(
                builder => { },
                builder => builder.Entity(
                    "People", e =>
                    {
                        // CHANGE - added column orders
                        e.Property<int>("Id").HasColumnOrder(0);
                        e.Property<int>("X").HasColumnOrder(1);
                        e.Property<int>("Y").HasColumnOrder(2);
                        // LAST CHANGE - added column order and changed type to int
                        e.Property<int>("Sum").HasComputedColumnSql(
                            $"{DelimitIdentifier("X")} + {DelimitIdentifier("Y")}",
                            stored).HasColumnOrder(3);
                    }),
                model =>
                {
                    var table = Assert.Single(model.Tables);
                    var sumColumn = Assert.Single(table.Columns, c => c.Name == "Sum");
                    if (AssertComputedColumns)
                    {
                        Assert.Contains("X", sumColumn.ComputedColumnSql);
                        Assert.Contains("Y", sumColumn.ComputedColumnSql);
                        if (stored != null)
                        {
                            Assert.Equal(stored, sumColumn.IsStored);
                        }
                    }
                });

        if (stored == true)
        {
            // Stored generated columns aren't supported
            await Assert.ThrowsAsync<NotSupportedException>(TestImpl);
            return;
        }

        await TestImpl();
    }

    public override async Task SqlOperation()
    {
        await Test(
            builder => { },
            // CHANGED SQL because of .net snowflake connector bug
            new SqlOperation { Sql = "SELECT 1;" },
            model =>
            {
                Assert.Empty(model.Tables);
                Assert.Empty(model.Sequences);
            });

        // CHANGED SQL
        AssertSql(
            """
            SELECT 1;
            """);
    }

    protected override void AssertSql(params string[] expected)
    {
        Fixture.TestSqlLoggerFactory.AssertSql(expected);
    }

    public class MigrationsSnowflakeFixture : MigrationsFixtureBase
    {
        protected override string StoreName
            => nameof(MigrationsSnowflakeTest);

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        public override RelationalTestHelpers TestHelpers
            => SnowflakeTestHelpers.Instance;

        public override Task InitializeAsync()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
            return base.InitializeAsync();
        }

        protected override IServiceCollection AddServices(IServiceCollection serviceCollection)
            => base.AddServices(serviceCollection)
                .AddScoped<IDatabaseModelFactory, SnowflakeDatabaseModelFactory>();
    }
}

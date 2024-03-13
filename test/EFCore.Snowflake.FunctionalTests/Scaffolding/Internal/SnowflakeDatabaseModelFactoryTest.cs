using System.Diagnostics;
using EFCore.Snowflake.Diagnostics.Internal;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Properties.Internal;
using EFCore.Snowflake.Scaffolding.Internal;
using EFCore.Snowflake.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.Logging;
using Snowflake.Data.Client;

namespace EFCore.Snowflake.FunctionalTests.Scaffolding.Internal;

public class SnowflakeDatabaseModelFactoryTest : IClassFixture<SnowflakeDatabaseModelFactoryTest.SnowflakeDatabaseModelFixture>
{
    private readonly SnowflakeDatabaseModelFixture _fixture;

    public SnowflakeDatabaseModelFactoryTest(SnowflakeDatabaseModelFixture fixture)
    {
        _fixture = fixture;
        _fixture.ListLoggerFactory.Clear();
    }

    #region Sequences

    [Fact]
    public void Create_sequences_with_facets()
        => Test([
            """
                CREATE SEQUENCE "DefaultFacetsSequence";
            """,
            """
                CREATE SEQUENCE "db2"."CustomFacetsSequence"
                    START WITH 1
                    INCREMENT BY 2;
            """],
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var defaultSequence = dbModel.Sequences.First(ds => ds.Name == "DefaultFacetsSequence");
                Assert.Equal("PUBLIC", defaultSequence.Schema);
                Assert.Equal("DefaultFacetsSequence", defaultSequence.Name);
                Assert.Equal("NUMBER(38,0)", defaultSequence.StoreType);
                Assert.False(defaultSequence.IsCyclic);
                Assert.Null(defaultSequence.IncrementBy);
                Assert.Null(defaultSequence.StartValue);
                Assert.Null(defaultSequence.MinValue);
                Assert.Null(defaultSequence.MaxValue);

                var customSequence = dbModel.Sequences.First(ds => ds.Name == "CustomFacetsSequence");
                Assert.Equal("db2", customSequence.Schema);
                Assert.Equal("CustomFacetsSequence", customSequence.Name);
                Assert.Equal("NUMBER(38,0)", customSequence.StoreType);
                Assert.False(customSequence.IsCyclic);
                Assert.Equal(2, customSequence.IncrementBy);
                Assert.Equal(1, customSequence.StartValue);
                Assert.Null(customSequence.MinValue);
                Assert.Null(customSequence.MaxValue);
            },
[
            """DROP SEQUENCE "DefaultFacetsSequence";""",
    """DROP SEQUENCE "db2"."CustomFacetsSequence";"""
        ]);


    [Fact]
    public void Filter_sequences_based_on_schema()
        => Test([
            """CREATE SEQUENCE "Sequence";""",
            """CREATE SEQUENCE "db2"."Sequence";"""
            ],
            Enumerable.Empty<string>(),
            new[] { "db2" },
            dbModel =>
            {
                var sequence = Assert.Single(dbModel.Sequences);
                Assert.Equal("db2", sequence.Schema);
                Assert.Equal("Sequence", sequence.Name);
                Assert.Equal("NUMBER(38,0)", sequence.StoreType);
            },
            [
                """DROP SEQUENCE "Sequence";""",
                """DROP SEQUENCE "db2"."Sequence";"""
            ]);
    #endregion

    #region Model
    [Fact]
    public void Set_default_schema()
        => Test(
            dbModel =>
            {
                var defaultSchema = _fixture.TestStore.ExecuteScalar<string>("SELECT CURRENT_SCHEMA()");
                Assert.Equal(defaultSchema, dbModel.DefaultSchema);
            });

    [Fact]
    public void Create_tables()
        => Test(
            ["""CREATE TABLE "Everest" (id int);""", """CREATE TABLE "Denali" (id int);"""],
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                Assert.Collection(
                    dbModel.Tables.OrderBy(t => t.Name),
                    d =>
                    {
                        Assert.Equal("PUBLIC", d.Schema);
                        Assert.Equal("Denali", d.Name);
                    },
                    e =>
                    {
                        Assert.Equal("PUBLIC", e.Schema);
                        Assert.Equal("Everest", e.Name);
                    });
            },
            ["""DROP TABLE "Everest";""", """DROP TABLE "Denali";"""]);
    #endregion

    #region FilteringSchemaTable

    [Fact]
    public void Filter_schemas()
        => Test(
            ["""CREATE TABLE "db2"."K2" (Id int, A varchar, UNIQUE (A));""", """CREATE TABLE "Kilimanjaro" (Id int, B varchar, UNIQUE (B));"""],
            Enumerable.Empty<string>(),
            new[] { "db2" },
            dbModel =>
            {
                var table = Assert.Single(dbModel.Tables);
                Assert.Equal("K2", table.Name);
                Assert.Equal(2, table.Columns.Count);
                Assert.Single(table.UniqueConstraints);
                Assert.Empty(table.ForeignKeys);
            },
            ["""DROP TABLE "Kilimanjaro";""", """DROP TABLE "db2"."K2"; """]);


    [Fact]
    public void Filter_tables()
        => Test(
            [
                """CREATE TABLE "K2" (Id int, A varchar, UNIQUE (A));""",
                """CREATE TABLE "Kilimanjaro" (Id int, B varchar, UNIQUE (B), FOREIGN KEY (B) REFERENCES "K2" (A));"""],
            ["K2"],
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = Assert.Single(dbModel.Tables);
                Assert.Equal("K2", table.Name);
                Assert.Equal(2, table.Columns.Count);
                Assert.Single(table.UniqueConstraints);
                Assert.Empty(table.ForeignKeys);
            },
            [
                """DROP TABLE "Kilimanjaro";""",
                """DROP TABLE "K2"; """]);

    [Fact]
    public void Filter_tables_with_qualified_name()
        => Test(
            [
                """CREATE TABLE "K.2" (Id int, A varchar, UNIQUE (A));""",
                """CREATE TABLE "Kilimanjaro" (Id int, B varchar, UNIQUE (B));"""
            ],
            new[] { @"""K.2""" },
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = Assert.Single(dbModel.Tables);
                Assert.Equal("K.2", table.Name);
                Assert.Equal(2, table.Columns.Count);
                Assert.Single(table.UniqueConstraints);
                Assert.Empty(table.ForeignKeys);
            },
            [
                """DROP TABLE "Kilimanjaro";""",
                """DROP TABLE "K.2";"""
            ]);


    [Fact]
    public void Filter_tables_with_schema_qualified_name1()
        => Test(
            [
                """CREATE TABLE PUBLIC."K2" (Id int, A varchar, UNIQUE (A));""",
                """CREATE TABLE "db2"."K2" (Id int, A varchar, UNIQUE (A));""",
                """CREATE TABLE "Kilimanjaro" (Id int, B varchar, UNIQUE (B));"""
            ],
            new[] { "PUBLIC.K2" },
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = Assert.Single(dbModel.Tables);
                Assert.Equal("K2", table.Name);
                Assert.Equal(2, table.Columns.Count);
                Assert.Single(table.UniqueConstraints);
                Assert.Empty(table.ForeignKeys);
            },
            [
            """DROP TABLE "Kilimanjaro";""",
                """DROP TABLE "K2";""",
                """DROP TABLE "db2"."K2";"""
            ]);


    [Fact]
    public void Filter_tables_with_schema_qualified_name2()
        => Test(
            [
                """CREATE TABLE "K.2" (Id int, A varchar, UNIQUE (A));""",
                """CREATE TABLE "db.2"."K.2" (Id int, A varchar, UNIQUE (A));""",
                """CREATE TABLE "db.2"."Kilimanjaro" (Id int, B varchar, UNIQUE (B));"""
            ],
            new[] { @"""db.2"".""K.2""" },
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = Assert.Single(dbModel.Tables);
                Assert.Equal("K.2", table.Name);
                Assert.Equal(2, table.Columns.Count);
                Assert.Single(table.UniqueConstraints);
                Assert.Empty(table.ForeignKeys);
            },
            [
                """DROP TABLE "db.2"."Kilimanjaro";""",
                """DROP TABLE "K.2";""",
                """DROP TABLE "db.2"."K.2";"""
            ]);

    [Fact]
    public void Filter_tables_with_schema_qualified_name3()
        => Test(
            [
                """CREATE TABLE "K.2" (Id int, A varchar, UNIQUE (A));""",
                """CREATE TABLE "db2"."K.2" (Id int, A varchar, UNIQUE (A));""",
                """CREATE TABLE "Kilimanjaro" (Id int, B varchar, UNIQUE (B));"""
            ],
            new[] { @"PUBLIC.""K.2""" },
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = Assert.Single(dbModel.Tables);
                Assert.Equal("K.2", table.Name);
                Assert.Equal(2, table.Columns.Count);
                Assert.Single(table.UniqueConstraints);
                Assert.Empty(table.ForeignKeys);
            },
            [
                """DROP TABLE "Kilimanjaro";""",
                """DROP TABLE "K.2";""",
                """DROP TABLE "db2"."K.2";"""
            ]);


    [Fact]
    public void Filter_tables_with_schema_qualified_name4()
        => Test(
            [
                """CREATE TABLE "K2" (Id int, A varchar, UNIQUE (A));""",
                """CREATE TABLE "db.2"."K2" (Id int, A varchar, UNIQUE (A));""",
                """CREATE TABLE "db.2"."Kilimanjaro" (Id int, B varchar, UNIQUE (B));"""
            ],
            new[] { @"""db.2"".K2" },
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = Assert.Single(dbModel.Tables);
                Assert.Equal("K2", table.Name);
                Assert.Equal(2, table.Columns.Count);
                Assert.Single(table.UniqueConstraints);
                Assert.Empty(table.ForeignKeys);
            },
            [
                """DROP TABLE "db.2"."Kilimanjaro";""",
                """DROP TABLE "K2";""",
                """DROP TABLE "db.2"."K2";"""
            ]);


    [Fact]
    public void Complex_filtering_validation()
        => Test(
            [
                """CREATE SEQUENCE PUBLIC."Sequence";""",
                """CREATE SEQUENCE "db2"."Sequence";""",

                """CREATE TABLE "db.2"."QuotedTableName" ("Id" int PRIMARY KEY);""",
                """CREATE TABLE "db.2"."Table.With.Dot" ("Id" int PRIMARY KEY);""",
                """CREATE TABLE "db.2"."SimpleTableName" ("Id" int PRIMARY KEY);""",
                """CREATE TABLE "db.2"."JustTableName" ("Id" int PRIMARY KEY);""",

                """CREATE TABLE PUBLIC."QuotedTableName" ("Id" int PRIMARY KEY);""",
                """CREATE TABLE PUBLIC."Table.With.Dot" ("Id" int PRIMARY KEY);""",
                """CREATE TABLE PUBLIC."SimpleTableName" ("Id" int PRIMARY KEY);""",
                """CREATE TABLE PUBLIC."JustTableName" ("Id" int PRIMARY KEY);""",

                """CREATE TABLE "db2"."QuotedTableName" ("Id" int PRIMARY KEY);""",
                """CREATE TABLE "db2"."Table.With.Dot" ("Id" int PRIMARY KEY);""",
                """CREATE TABLE "db2"."SimpleTableName" ("Id" int PRIMARY KEY);""",
                """CREATE TABLE "db2"."JustTableName" ("Id" int PRIMARY KEY);""",

                @"CREATE TABLE ""db2"".""PrincipalTable"" (
                    ""Id"" int PRIMARY KEY,
                    ""UC1"" text,
                    ""UC2"" int,
                    ""Index1"" boolean,
                    ""Index2"" bigint,
                    CONSTRAINT ""UX"" UNIQUE (""UC1"", ""UC2"")
                );",

                @"CREATE TABLE ""db2"".""DependentTable""(
                    ""Id"" int PRIMARY KEY,
                    ""ForeignKeyId1"" text,
                    ""ForeignKeyId2"" int,
                    FOREIGN KEY (""ForeignKeyId1"", ""ForeignKeyId2"") REFERENCES ""db2"".""PrincipalTable""(""UC1"", ""UC2"") ON DELETE NO ACTION
                );"
            ],
            new[]
            {
                @"""db.2"".""QuotedTableName""",
                @"""db.2"".SimpleTableName",
                @"PUBLIC.""Table.With.Dot""",
                @"PUBLIC.""SimpleTableName""",
                @"""JustTableName"""
            },
            new[] { "db2" },
            dbModel =>
            {
                var sequence = Assert.Single(dbModel.Sequences);
                Assert.Equal("db2", sequence.Schema);

                Assert.Single(dbModel.Tables.Where(t => t.Schema == "db.2" && t.Name == "QuotedTableName"));
                Assert.Empty(dbModel.Tables.Where(t => t.Schema == "db.2" && t.Name == "Table.With.Dot"));
                Assert.Single(dbModel.Tables.Where(t => t.Schema == "db.2" && t.Name == "SimpleTableName"));
                Assert.Single(dbModel.Tables.Where(t => t.Schema == "db.2" && t.Name == "JustTableName"));

                Assert.Empty(dbModel.Tables.Where(t => t.Schema == "PUBLIC" && t.Name == "QuotedTableName"));
                Assert.Single(dbModel.Tables.Where(t => t.Schema == "PUBLIC" && t.Name == "Table.With.Dot"));
                Assert.Single(dbModel.Tables.Where(t => t.Schema == "PUBLIC" && t.Name == "SimpleTableName"));
                Assert.Single(dbModel.Tables.Where(t => t.Schema == "PUBLIC" && t.Name == "JustTableName"));

                Assert.Single(dbModel.Tables.Where(t => t.Schema == "db2" && t.Name == "QuotedTableName"));
                Assert.Single(dbModel.Tables.Where(t => t.Schema == "db2" && t.Name == "Table.With.Dot"));
                Assert.Single(dbModel.Tables.Where(t => t.Schema == "db2" && t.Name == "SimpleTableName"));
                Assert.Single(dbModel.Tables.Where(t => t.Schema == "db2" && t.Name == "JustTableName"));

                var principalTable = Assert.Single(dbModel.Tables.Where(t => t.Schema == "db2" && t.Name == "PrincipalTable"));
                Assert.NotNull(principalTable.PrimaryKey);
                Assert.Single(principalTable.UniqueConstraints);
                Assert.Empty(principalTable.Indexes);

                var dependentTable = Assert.Single(dbModel.Tables.Where(t => t.Schema == "db2" && t.Name == "DependentTable"));
                Assert.Single(dependentTable.ForeignKeys);
            },
            [
                """DROP SEQUENCE PUBLIC."Sequence";""",
                """DROP SEQUENCE "db2"."Sequence";""",

                """DROP TABLE "db.2"."QuotedTableName";""",
                """DROP TABLE "db.2"."Table.With.Dot"; """,
                """DROP TABLE "db.2"."SimpleTableName";""",
                """DROP TABLE "db.2"."JustTableName";  """,

                """DROP TABLE PUBLIC."QuotedTableName";""",
                """DROP TABLE PUBLIC."Table.With.Dot"; """,
                """DROP TABLE PUBLIC."SimpleTableName";""",
                """DROP TABLE PUBLIC."JustTableName";""",

                """DROP TABLE "db2"."QuotedTableName";""",
                """DROP TABLE "db2"."Table.With.Dot";""",
                """DROP TABLE "db2"."SimpleTableName";""",
                """DROP TABLE "db2"."JustTableName";""",
                """DROP TABLE "db2"."DependentTable";""",
                """DROP TABLE "db2"."PrincipalTable";"""
            ]);


    #endregion

    #region Table

    [Fact]
    public void Create_columns()
        => Test(
            """
            CREATE TABLE "Blogs" (
                "Id" int,
                "Name" text NOT NULL
            );
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = dbModel.Tables.Single();

                Assert.Equal(2, table.Columns.Count);
                Assert.All(
                    table.Columns, c =>
                    {
                        Assert.Equal("PUBLIC", c.Table.Schema);
                        Assert.Equal("Blogs", c.Table.Name);
                    });

                Assert.Single(table.Columns.Where(c => c.Name == "Id"));
                Assert.Single(table.Columns.Where(c => c.Name == "Name"));
            },
            @"DROP TABLE ""Blogs""");


    [Fact]
    public void Create_view_columns()
        => Test(
            """
            CREATE VIEW "BlogsView" AS SELECT 100::int AS "Id", ''::text AS "Name";
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = Assert.IsType<DatabaseView>(dbModel.Tables.Single());

                Assert.Equal(2, table.Columns.Count);
                Assert.Null(table.PrimaryKey);
                Assert.All(
                    table.Columns, c =>
                    {
                        Assert.Equal("PUBLIC", c.Table.Schema);
                        Assert.Equal("BlogsView", c.Table.Name);
                    });

                Assert.Single(table.Columns.Where(c => c.Name == "Id"));
                Assert.Single(table.Columns.Where(c => c.Name == "Name"));
            },
            @"DROP VIEW ""BlogsView"";");

    [Fact]
    public void Create_primary_key()
        => Test(
            """
            CREATE TABLE "PrimaryKeyTable" ("Id" int PRIMARY KEY);
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var pk = dbModel.Tables.Single().PrimaryKey;

                Assert.Equal("PUBLIC", pk?.Table?.Schema);
                Assert.Equal("PrimaryKeyTable", pk!.Table?.Name);
                Assert.StartsWith("SYS_CONSTRAINT_", pk.Name);
                Assert.Equal(new List<string> { "Id" }, pk.Columns.Select(ic => ic.Name).ToList());
            },
            @"DROP TABLE ""PrimaryKeyTable""");

    [Fact]
    public void Create_unique_constraints()
        => Test(
            """
            CREATE TABLE "UniqueConstraint" (
                "Id" int,
                "Name" int Unique,
                "IndexProperty" int,
                "Unq1" int,
                "Unq2" int,
                UNIQUE ("Unq1", "Unq2")
            );

            
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = dbModel.Tables.Single();
                Assert.Equal(2, table.UniqueConstraints.Count);

                var firstConstraint = table.UniqueConstraints.Single(c => c.Columns.Count == 1);
                Assert.Equal("PUBLIC", firstConstraint.Table.Schema);
                Assert.Equal("UniqueConstraint", firstConstraint.Table.Name);
                Assert.StartsWith("SYS_CONSTRAINT_", firstConstraint.Name);
                Assert.Equal(new List<string> { "Name" }, firstConstraint.Columns.Select(ic => ic.Name).ToList());

                var secondConstraint = table.UniqueConstraints.Single(c => c.Columns.Count == 2);
                Assert.Equal("PUBLIC", secondConstraint.Table.Schema);
                Assert.Equal("UniqueConstraint", secondConstraint.Table.Name);
                Assert.StartsWith("SYS_CONSTRAINT_", secondConstraint.Name);
                Assert.Equal(new List<string> { "Unq1", "Unq2" }, secondConstraint.Columns.Select(ic => ic.Name).ToList());
            },
            @"DROP TABLE ""UniqueConstraint""");


    [Fact]
    public void Create_foreign_keys()
        => Test(
            [$@"
                CREATE TABLE ""PrincipalTable"" (
                    ""Id"" int PRIMARY KEY
                );",
                @"
                CREATE TABLE ""FirstDependent"" (
                    ""Id"" int PRIMARY KEY,
                    ""ForeignKeyId"" int,
                    FOREIGN KEY (""ForeignKeyId"") REFERENCES ""PrincipalTable""(""Id"") ON DELETE NO ACTION
                );",
                @$"
                CREATE TABLE ""SecondDependent"" (
                    ""Id"" int PRIMARY KEY,
                    FOREIGN KEY (""Id"") REFERENCES ""PrincipalTable""(""Id"") ON DELETE NO ACTION
                );"
            ],
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var firstFk = Assert.Single(dbModel.Tables.Single(t => t.Name == "FirstDependent").ForeignKeys);

                Assert.Equal("PUBLIC", firstFk.Table.Schema);
                Assert.Equal("FirstDependent", firstFk.Table.Name);
                Assert.Equal("PUBLIC", firstFk.PrincipalTable.Schema);
                Assert.Equal("PrincipalTable", firstFk.PrincipalTable.Name);
                Assert.Equal(new List<string> { "ForeignKeyId" }, firstFk.Columns.Select(ic => ic.Name).ToList());
                Assert.Equal(new List<string> { "Id" }, firstFk.PrincipalColumns.Select(ic => ic.Name).ToList());
                Assert.Equal(ReferentialAction.NoAction, firstFk.OnDelete);

                var secondFk = Assert.Single(dbModel.Tables.Single(t => t.Name == "SecondDependent").ForeignKeys);

                Assert.Equal("PUBLIC", secondFk.Table.Schema);
                Assert.Equal("SecondDependent", secondFk.Table.Name);
                Assert.Equal("PUBLIC", secondFk.PrincipalTable.Schema);
                Assert.Equal("PrincipalTable", secondFk.PrincipalTable.Name);
                Assert.Equal(new List<string> { "Id" }, secondFk.Columns.Select(ic => ic.Name).ToList());
                Assert.Equal(new List<string> { "Id" }, secondFk.PrincipalColumns.Select(ic => ic.Name).ToList());
                Assert.Equal(ReferentialAction.NoAction, secondFk.OnDelete);
            },
            [
                """DROP TABLE "SecondDependent";""",
                """DROP TABLE "FirstDependent";""",
                """DROP TABLE "PrincipalTable";"""
            ]);

    #endregion

    #region ColumnFacets

    // Note: in Snowflake decimal or numeric is simply an alias for NUMBER
    [Fact]
    public void Decimal_numeric_types_have_precision_scale()
        => Test(
            """
            CREATE TABLE "NumericColumns" (
                "Id" int,
                "numericColumn" NUMERIC NOT NULL,
                "numeric152Column" NUMERIC(15, 2) NOT NULL,
                "numeric18Column" NUMBER(18) NOT NULL
            )
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;

                Assert.Equal("NUMBER(38,0)", columns.Single(c => c.Name == "numericColumn").StoreType);
                Assert.Equal("NUMBER(15,2)", columns.Single(c => c.Name == "numeric152Column").StoreType);
                Assert.Equal("NUMBER(18,0)", columns.Single(c => c.Name == "numeric18Column").StoreType);
            },
            @"DROP TABLE ""NumericColumns""");

    [Fact]
    public void Specific_max_length_are_add_to_store_type()
        => Test(
            """
            CREATE TABLE "LengthColumns" (
                "Id" int,
                "char10Column" char(10) NULL,
                "varchar66Column" varchar(66) NULL
            )
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;

                Assert.Equal("VARCHAR(10)", columns.Single(c => c.Name == "char10Column").StoreType);
                Assert.Equal("VARCHAR(66)", columns.Single(c => c.Name == "varchar66Column").StoreType);
            },
            @"DROP TABLE ""LengthColumns""");


    [Fact]
    public void Datetime_types_have_precision_if_non_null_scale()
        => Test(
            """
            CREATE TABLE "LengthColumns" (
                "Id" int,
                "time1Column" time(1) NULL,
                "dateCol" date NULL,
                "timestamp_tz2Column" TIMESTAMP_TZ(2) NULL,
                "timestamp_ntz3Column" TIMESTAMP_NTZ(3) NULL,
                "timestamp_ltz4Column" TIMESTAMP_LTZ(4) NULL
            )
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;

                Assert.Equal("TIME(1)", columns.Single(c => c.Name == "time1Column").StoreType);
                Assert.Equal("DATE", columns.Single(c => c.Name == "dateCol").StoreType);
                Assert.Equal("TIMESTAMP_TZ(2)", columns.Single(c => c.Name == "timestamp_tz2Column").StoreType);
                Assert.Equal("TIMESTAMP_NTZ(3)", columns.Single(c => c.Name == "timestamp_ntz3Column").StoreType);
                Assert.Equal("TIMESTAMP_LTZ(4)", columns.Single(c => c.Name == "timestamp_ltz4Column").StoreType);
            },
            @"DROP TABLE ""LengthColumns""");

    [Fact]
    public void Binary_columns_have_length_provided()
        => Test(
            """
            CREATE TABLE "BinaryColumns" (
                "Id" int,
                "defaultBinaryCol" BINARY NULL,
                "fixedLengthCol" BINARY(123) NULL
            )
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;

                Assert.Equal("BINARY(8388608)", columns.Single(c => c.Name == "defaultBinaryCol").StoreType);
                Assert.Equal("BINARY(123)", columns.Single(c => c.Name == "fixedLengthCol").StoreType);
            },
            @"DROP TABLE ""BinaryColumns""");

    [Fact]
    public void Default_values_are_stored()
        => Test(
            """
            CREATE TABLE "DefaultValues" (
                "Id" int,
                "FixedDefaultValue" TIMESTAMP_NTZ NOT NULL DEFAULT ('2014-01-01 16:00:00'::TIMESTAMP_NTZ)
            )
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;
                Assert.Equal(
                    "CAST('2014-01-01 16:00:00' AS TIMESTAMP_NTZ(9))",
                    columns.Single(c => c.Name == "FixedDefaultValue").DefaultValueSql);
            },
            @"DROP TABLE ""DefaultValues""");

    [Fact]
    public void Deserializes_clrs_defaults()
        => Test(
            """
            CREATE TABLE "BooleanDefault" (
                "Id" int,
                FOO BOOLEAN DEFAULT FALSE,
                SHORT NUMBER(4,0) DEFAULT 12,
                INT NUMBER(7,0) DEFAULT -123,
                DECIMAL NUMBER(16,2) DEFAULT 123.45,
                FLOATING DOUBLE DEFAULT -5.5
            )
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                IList<DatabaseColumn> columns = dbModel.Tables.Single().Columns;
                DatabaseColumn foo = columns.Single(c => c.Name == "FOO");
                DatabaseColumn shortCol = columns.Single(c => c.Name == "SHORT");
                DatabaseColumn intCol = columns.Single(c => c.Name == "INT");
                DatabaseColumn decimalCol = columns.Single(c => c.Name == "DECIMAL");
                DatabaseColumn doubleCol = columns.Single(c => c.Name == "FLOATING");
                Assert.Equal("FALSE", foo.DefaultValueSql);
                Assert.Equal(false, foo.DefaultValue);

                Assert.Equal((short)12, shortCol.DefaultValue);
                Assert.Equal(-123, intCol.DefaultValue);
                Assert.Equal(123.45m, decimalCol.DefaultValue);
                Assert.Equal(-5.5, doubleCol.DefaultValue);
            },
            @"DROP TABLE ""BooleanDefault""");

    [Fact]
    public void Computed_values_are_stored()
        => Test(
            """
            CREATE TABLE "ComputedValues" (
                "Id" int,
                "Derived" bigint as ("Id" * 10)
            )
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;

                var idColumn = columns.Single(c => c.Name == "Id");
                Assert.Null(idColumn.DefaultValueSql);
                Assert.Null(idColumn.ComputedColumnSql);

                var column = columns.Single(c => c.Name == "Derived");
                Assert.Null(column.DefaultValueSql);
                Assert.Equal(@"""Id"" * 10", column.ComputedColumnSql);
                Assert.False(column.IsStored);
            },
            @"DROP TABLE ""ComputedValues""");


    [Fact]
    public void ValueGenerated_is_set_for_auto_increment_but_not_default()
        => Test(
            """
            CREATE TABLE "ValueGeneratedProperties" (
                "Id" bigint AUTOINCREMENT START 1 INCREMENT 1,
                "NoValueGenerationColumn" text,
                "FixedDefaultValue" TIMESTAMP_NTZ NOT NULL DEFAULT ('2014-01-01 16:00:00'::TIMESTAMP_NTZ)
            )
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;

                Assert.Equal(ValueGenerated.OnAdd, columns.Single(c => c.Name == "Id").ValueGenerated);
                Assert.Null(columns.Single(c => c.Name == "NoValueGenerationColumn").ValueGenerated);
                Assert.Null(columns.Single(c => c.Name == "FixedDefaultValue").ValueGenerated);
            },
            @"DROP TABLE ""ValueGeneratedProperties""");

    [Fact]
    public void Column_nullability_is_set()
        => Test(
            """
            CREATE TABLE "NullableColumns" (
                "Id" int,
                "NullableInt" int NULL,
                "NonNullableInt" int NOT NULL
            )
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;

                Assert.True(columns.Single(c => c.Name == "NullableInt").IsNullable);
                Assert.False(columns.Single(c => c.Name == "NonNullableInt").IsNullable);
            },
            @"DROP TABLE ""NullableColumns""");

    [Fact]
    public void Column_collation_is_set()
        => Test(
            """
CREATE TABLE "ColumnsWithCollation" (
    "Id" int,
    "DefaultCollation" VARCHAR,
    "NonDefaultCollation" VARCHAR COLLATE 'de-ci-pi'
);
""",
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var columns = dbModel.Tables.Single().Columns;

                Assert.Null(columns.Single(c => c.Name == "DefaultCollation").Collation);
                Assert.Equal("de-ci-pi", columns.Single(c => c.Name == "NonDefaultCollation").Collation);
            },
            """DROP TABLE "ColumnsWithCollation";""");

    #endregion

    #region PrimaryKeyFacets

    [Fact]
    public void Create_composite_primary_key()
        => Test(
            """
            CREATE TABLE "CompositePrimaryKeyTable" (
                "Id1" int,
                "Id2" int,
                PRIMARY KEY ("Id2", "Id1")
            )
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var pk = dbModel.Tables.Single().PrimaryKey;

                Assert.Equal("PUBLIC", pk?.Table?.Schema);
                Assert.Equal("CompositePrimaryKeyTable", pk!.Table!.Name);
                Assert.Equal(new List<string> { "Id2", "Id1" }, pk.Columns.Select(ic => ic.Name).ToList());
            },
            @"DROP TABLE ""CompositePrimaryKeyTable""");

    [Fact]
    public void Set_primary_key_name_from_index()
        => Test(
            """
            CREATE TABLE "PrimaryKeyName" (
                "Id1" int,
                "Id2" int,
                CONSTRAINT "MyPK" PRIMARY KEY ( "Id2" )
            )
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var pk = dbModel.Tables.Single().PrimaryKey;

                Assert.Equal("PUBLIC", pk?.Table?.Schema);
                Assert.Equal("PrimaryKeyName", pk!.Table!.Name);
                Assert.StartsWith("MyPK", pk.Name);
                Assert.Equal(new List<string> { "Id2" }, pk.Columns.Select(ic => ic.Name).ToList());
            },
            @"DROP TABLE ""PrimaryKeyName""");

    #endregion

    #region UniqueConstraintFacets

    [Fact]
    public void Create_composite_unique_constraint()
        => Test(
            """
            CREATE TABLE "CompositeUniqueConstraintTable" (
                "Id1" int,
                "Id2" int,
                CONSTRAINT "UX" UNIQUE ("Id2", "Id1")
            );
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var uniqueConstraint = Assert.Single(dbModel.Tables.Single().UniqueConstraints);

                Assert.Equal("PUBLIC", uniqueConstraint.Table.Schema);
                Assert.Equal("CompositeUniqueConstraintTable", uniqueConstraint.Table.Name);
                Assert.Equal("UX", uniqueConstraint.Name);
                Assert.Equal(new List<string> { "Id2", "Id1" }, uniqueConstraint.Columns.Select(ic => ic.Name).ToList());
            },
            @"DROP TABLE ""CompositeUniqueConstraintTable""");


    [Fact]
    public void Set_unique_constraint_name_from_index()
        => Test(
            """
            CREATE TABLE "UniqueConstraintName" (
                "Id1" int,
                "Id2" int,
                CONSTRAINT "MyUC" UNIQUE ( "Id2" )
            );
            """,
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var table = dbModel.Tables.Single();
                var uniqueConstraint = Assert.Single(table.UniqueConstraints);

                Assert.Equal("PUBLIC", uniqueConstraint.Table.Schema);
                Assert.Equal("UniqueConstraintName", uniqueConstraint.Table.Name);
                Assert.Equal("MyUC", uniqueConstraint.Name);
                Assert.Equal(new List<string> { "Id2" }, uniqueConstraint.Columns.Select(ic => ic.Name).ToList());
                Assert.Empty(table.Indexes);
            },
            @"DROP TABLE ""UniqueConstraintName""");

    #endregion

    #region ForeignKeyFacets

    [Fact]
    public void Create_composite_foreign_key()
        => Test([
            """
            CREATE TABLE "PrincipalTable" (
                "Id1" int,
                "Id2" int,
                PRIMARY KEY ("Id1", "Id2")
            );
            """,
            """
            CREATE TABLE "DependentTable" (
                "Id" int PRIMARY KEY,
                "ForeignKeyId1" int,
                "ForeignKeyId2" int,
                FOREIGN KEY ("ForeignKeyId1", "ForeignKeyId2") REFERENCES "PrincipalTable"("Id1", "Id2")
            );
            """],
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var fk = Assert.Single(dbModel.Tables.Single(t => t.Name == "DependentTable").ForeignKeys);

                Assert.Equal("PUBLIC", fk.Table.Schema);
                Assert.Equal("DependentTable", fk.Table.Name);
                Assert.Equal("PUBLIC", fk.PrincipalTable.Schema);
                Assert.Equal("PrincipalTable", fk.PrincipalTable.Name);
                Assert.Equal(new List<string> { "ForeignKeyId1", "ForeignKeyId2" }, fk.Columns.Select(ic => ic.Name).ToList());
                Assert.Equal(new List<string> { "Id1", "Id2" }, fk.PrincipalColumns.Select(ic => ic.Name).ToList());
                Assert.Equal(ReferentialAction.NoAction, fk.OnDelete);
            },
            [
                """DROP TABLE "DependentTable";""",
                """DROP TABLE "PrincipalTable";"""
            ]);


    [Fact]
    public void Create_multiple_foreign_key_in_same_table()
        => Test([
            """
            CREATE TABLE "PrincipalTable" (
                "Id" int PRIMARY KEY
            );
            """,
            """
            CREATE TABLE "AnotherPrincipalTable" (
                "Id" int PRIMARY KEY
            );
            """,
            """
            CREATE TABLE "DependentTable" (
                "Id" int PRIMARY KEY,
                "ForeignKeyId1" int,
                "ForeignKeyId2" int,
                FOREIGN KEY ("ForeignKeyId1") REFERENCES "PrincipalTable"("Id") ON DELETE NO ACTION,
                FOREIGN KEY ("ForeignKeyId2") REFERENCES "AnotherPrincipalTable"("Id") ON DELETE NO ACTION
            );
            """],
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var foreignKeys = dbModel.Tables.Single(t => t.Name == "DependentTable").ForeignKeys;

                Assert.Equal(2, foreignKeys.Count);

                var principalFk = Assert.Single(foreignKeys.Where(f => f.PrincipalTable.Name == "PrincipalTable"));

                Assert.Equal("PUBLIC", principalFk.Table.Schema);
                Assert.Equal("DependentTable", principalFk.Table.Name);
                Assert.Equal("PUBLIC", principalFk.PrincipalTable.Schema);
                Assert.Equal("PrincipalTable", principalFk.PrincipalTable.Name);
                Assert.Equal(new List<string> { "ForeignKeyId1" }, principalFk.Columns.Select(ic => ic.Name).ToList());
                Assert.Equal(new List<string> { "Id" }, principalFk.PrincipalColumns.Select(ic => ic.Name).ToList());
                Assert.Equal(ReferentialAction.NoAction, principalFk.OnDelete);

                var anotherPrincipalFk = Assert.Single(foreignKeys.Where(f => f.PrincipalTable.Name == "AnotherPrincipalTable"));

                Assert.Equal("PUBLIC", anotherPrincipalFk.Table.Schema);
                Assert.Equal("DependentTable", anotherPrincipalFk.Table.Name);
                Assert.Equal("PUBLIC", anotherPrincipalFk.PrincipalTable.Schema);
                Assert.Equal("AnotherPrincipalTable", anotherPrincipalFk.PrincipalTable.Name);
                Assert.Equal(new List<string> { "ForeignKeyId2" }, anotherPrincipalFk.Columns.Select(ic => ic.Name).ToList());
                Assert.Equal(new List<string> { "Id" }, anotherPrincipalFk.PrincipalColumns.Select(ic => ic.Name).ToList());
                Assert.Equal(ReferentialAction.NoAction, anotherPrincipalFk.OnDelete);
            },
            [
                """DROP TABLE "DependentTable";""",
                """DROP TABLE "AnotherPrincipalTable";""",
                """DROP TABLE "PrincipalTable";"""
            ]);

    [Fact]
    public void Create_foreign_key_referencing_unique_constraint()
        => Test([
            """
            CREATE TABLE "PrincipalTable" (
                "Id1" int,
                "Id2" int UNIQUE
            );
            """,
            """
            CREATE TABLE "DependentTable" (
                "Id" int PRIMARY KEY,
                "ForeignKeyId" int,
                FOREIGN KEY ("ForeignKeyId") REFERENCES "PrincipalTable"("Id2") ON DELETE NO ACTION
            );
            """],
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var fk = Assert.Single(dbModel.Tables.Single(t => t.Name == "DependentTable").ForeignKeys);

                // ReSharper disable once PossibleNullReferenceException
                Assert.Equal("PUBLIC", fk.Table.Schema);
                Assert.Equal("DependentTable", fk.Table.Name);
                Assert.Equal("PUBLIC", fk.PrincipalTable.Schema);
                Assert.Equal("PrincipalTable", fk.PrincipalTable.Name);
                Assert.Equal(new List<string> { "ForeignKeyId" }, fk.Columns.Select(ic => ic.Name).ToList());
                Assert.Equal(new List<string> { "Id2" }, fk.PrincipalColumns.Select(ic => ic.Name).ToList());
                Assert.Equal(ReferentialAction.NoAction, fk.OnDelete);
            },
            [
                """DROP TABLE "DependentTable";""",
                """DROP TABLE "PrincipalTable";"""
            ]);


    [Fact]
    public void Set_name_for_foreign_key()
        => Test(
            [
                """
                    CREATE TABLE "PrincipalTable" (
                        "Id" int PRIMARY KEY
                    );
                """,
                """
                     CREATE TABLE "DependentTable" (
                         "Id" int PRIMARY KEY,
                         "ForeignKeyId" int,
                         CONSTRAINT "MYFK" FOREIGN KEY ("ForeignKeyId") REFERENCES "PrincipalTable"("Id") ON DELETE NO ACTION
                     );
                 """],
            Enumerable.Empty<string>(),
            Enumerable.Empty<string>(),
            dbModel =>
            {
                var fk = Assert.Single(dbModel.Tables.Single(t => t.Name == "DependentTable").ForeignKeys);

                Assert.Equal("PUBLIC", fk.Table.Schema);
                Assert.Equal("DependentTable", fk.Table.Name);
                Assert.Equal("PUBLIC", fk.PrincipalTable.Schema);
                Assert.Equal("PrincipalTable", fk.PrincipalTable.Name);
                Assert.Equal(new List<string> { "ForeignKeyId" }, fk.Columns.Select(ic => ic.Name).ToList());
                Assert.Equal(new List<string> { "Id" }, fk.PrincipalColumns.Select(ic => ic.Name).ToList());
                Assert.Equal(ReferentialAction.NoAction, fk.OnDelete);
                // ReSharper disable once StringLiteralTypo
                Assert.Equal("MYFK", fk.Name);
            },
            [

                """DROP TABLE "DependentTable";""",
                """DROP TABLE "PrincipalTable";"""
            ]);


    #endregion

    #region Warnings

    [Fact]
    public void Warn_missing_schema()
        => Test(
            """
            CREATE TABLE "Blank" ("Id" int)
            """,
            Enumerable.Empty<string>(),
            new[] { "MySchema" },
            dbModel =>
            {
                Assert.Empty(dbModel.Tables);

                var (_, id, message, _, _) = Assert.Single(_fixture.ListLoggerFactory.Log.Where(t => t.Level == LogLevel.Warning));

                Assert.Equal(SnowflakeResources.LogMissingSchema(new TestLogger<SnowflakeLoggingDefinitions>()).EventId, id);
                Assert.Equal(
                    SnowflakeResources.LogMissingSchema(new TestLogger<SnowflakeLoggingDefinitions>()).GenerateMessage("MySchema"), message);
            },
            @"DROP TABLE ""Blank""");

    [Fact]
    public void Warn_missing_table()
        => Test(
            """
            CREATE TABLE "Blank" ("Id" int)
            """,
            new[] { "MyTable" },
            Enumerable.Empty<string>(),
            dbModel =>
            {
                Assert.Empty(dbModel.Tables);

                var (_, id, message, _, _) = Assert.Single(_fixture.ListLoggerFactory.Log.Where(t => t.Level == LogLevel.Warning));

                Assert.Equal(SnowflakeResources.LogMissingTable(new TestLogger<SnowflakeLoggingDefinitions>()).EventId, id);
                Assert.Equal(
                    SnowflakeResources.LogMissingTable(new TestLogger<SnowflakeLoggingDefinitions>()).GenerateMessage("MyTable"), message);
            },
            @"DROP TABLE ""Blank""");

    [Fact]
    public void Warn_missing_principal_table_for_foreign_key()
        => Test(
            ["""
            CREATE TABLE "PrincipalTable" (
                "Id" int PRIMARY KEY
            );
            """, """
            CREATE TABLE "DependentTable" (
                "Id" int PRIMARY KEY,
                "ForeignKeyId" int,
                CONSTRAINT "MYFK" FOREIGN KEY ("ForeignKeyId") REFERENCES "PrincipalTable"("Id") ON DELETE NO ACTION
            );
            """],
            new[] { "DependentTable" },
            Enumerable.Empty<string>(),
            _ =>
            {
                var (_, id, message, _, _) = Assert.Single(_fixture.ListLoggerFactory.Log.Where(t => t.Level == LogLevel.Warning));

                Assert.Equal(SnowflakeResources.LogPrincipalTableNotInSelectionSet(new TestLogger<SnowflakeLoggingDefinitions>()).EventId, id);
                Assert.Equal(
                    SnowflakeResources.LogPrincipalTableNotInSelectionSet(new TestLogger<SnowflakeLoggingDefinitions>()).GenerateMessage(
                        "MYFK", "PUBLIC.DependentTable", "PUBLIC.PrincipalTable"), message);
            },
            [
                """DROP TABLE "DependentTable";""",
                """DROP TABLE "PrincipalTable";"""
            ]);

    #endregion

    private void Test(
        Action<DatabaseModel> asserter,
        string[]? cleanupSql = null)
        => Test(
            Array.Empty<string>(),
            Array.Empty<string>(),
            Array.Empty<string>(),
            asserter,
            cleanupSql);

    private void Test(
        string? createSql,
        IEnumerable<string> tables,
        IEnumerable<string> schemas,
        Action<DatabaseModel> asserter,
        string? cleanupSql)
        => Test(
            string.IsNullOrEmpty(createSql) ? Array.Empty<string>() : [createSql],
            tables,
            schemas,
            asserter,
            string.IsNullOrEmpty(cleanupSql) ? Array.Empty<string>() : [cleanupSql]);

    private void Test(
        string[] createSqls,
        IEnumerable<string> tables,
        IEnumerable<string> schemas,
        Action<DatabaseModel> asserter,
        string[]? cleanupSql)
    {
        foreach (var createSql in createSqls)
        {
            _fixture.TestStore.ExecuteNonQuery(createSql);
        }

        try
        {
#pragma warning disable EF1001
            var databaseModelFactory = new SnowflakeDatabaseModelFactory(
                new SnowflakeSqlGenerationHelper(new RelationalSqlGenerationHelperDependencies()),
                new DiagnosticsLogger<DbLoggerCategory.Scaffolding>(
                    _fixture.ListLoggerFactory,
                    new LoggingOptions(),
                    new DiagnosticListener("Fake"),
                    new SnowflakeLoggingDefinitions(),
                    new NullDbContextLogger()));
#pragma warning restore EF1001

            DatabaseModel databaseModel = databaseModelFactory.Create(
                _fixture.TestStore.ConnectionString,
                new DatabaseModelFactoryOptions(tables, schemas));
            Assert.NotNull(databaseModel);
            asserter(databaseModel);
        }
        finally
        {
            if (cleanupSql != null)
            {
                foreach (var queryString in cleanupSql)
                {
                    _fixture.TestStore.ExecuteNonQuery(queryString);
                }
            }
        }
    }

    public class SnowflakeDatabaseModelFixture : SharedStoreFixtureBase<PoolableDbContext>
    {
        protected override string StoreName { get; } = nameof(SnowflakeDatabaseModelFactoryTest);

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        public new SnowflakeTestStore TestStore
            => (SnowflakeTestStore)base.TestStore;

        public override async Task InitializeAsync()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
            await base.InitializeAsync();
            await TestStore.ExecuteNonQueryAsync(@"CREATE SCHEMA IF NOT EXISTS ""db2""");
            await TestStore.ExecuteNonQueryAsync(@"CREATE SCHEMA IF NOT EXISTS ""db.2""");
        }

        protected override bool ShouldLogCategory(string logCategory)
            => logCategory == DbLoggerCategory.Scaffolding.Name;
    }
}

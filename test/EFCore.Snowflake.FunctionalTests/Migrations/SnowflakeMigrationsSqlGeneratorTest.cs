using System.Diagnostics;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using EFCore.Snowflake.Metadata;
using EFCore.Snowflake.Metadata.Internal;
using EFCore.Snowflake.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EFCore.Snowflake.FunctionalTests.Migrations;

public class SnowflakeMigrationsSqlGeneratorTest() : MigrationsSqlGeneratorTestBase(SnowflakeTestHelpers.Instance)
{
    public override void AddColumnOperation_without_column_type()
    {
        base.AddColumnOperation_without_column_type();

        AssertSql(
            """
            ALTER TABLE "People" ADD "Alias" VARCHAR(16777216) NOT NULL;
            """);
    }

    public override void AddColumnOperation_with_unicode_overridden()
    {
        base.AddColumnOperation_with_unicode_overridden();

        AssertSql(
            """
            ALTER TABLE "Person" ADD "Name" VARCHAR(16777216) NULL;
            """);
    }

    public override void AddColumnOperation_with_unicode_no_model()
    {
        base.AddColumnOperation_with_unicode_no_model();

        AssertSql(
            """
            ALTER TABLE "Person" ADD "Name" VARCHAR(16777216) NULL;
            """);
    }

    public override void AddColumnOperation_with_fixed_length_no_model()
    {
        base.AddColumnOperation_with_fixed_length_no_model();

        AssertSql(
            """
            ALTER TABLE "Person" ADD "Name" VARCHAR(100) NULL;
            """);
    }

    public override void AddColumnOperation_with_maxLength_overridden()
    {
        base.AddColumnOperation_with_maxLength_overridden();

        AssertSql(
            """
            ALTER TABLE "Person" ADD "Name" VARCHAR(32) NULL;
            """);
    }

    public override void AddColumnOperation_with_maxLength_no_model()
    {
        base.AddColumnOperation_with_maxLength_no_model();

        AssertSql(
            """
            ALTER TABLE "Person" ADD "Name" VARCHAR(30) NULL;
            """);
    }

    public override void AddColumnOperation_with_precision_and_scale_overridden()
    {
        base.AddColumnOperation_with_precision_and_scale_overridden();

        AssertSql(
            """
            ALTER TABLE "Person" ADD "Pi" NUMBER(15,10) NOT NULL;
            """);
    }

    public override void AddColumnOperation_with_precision_and_scale_no_model()
    {
        base.AddColumnOperation_with_precision_and_scale_no_model();

        AssertSql(
            """
            ALTER TABLE "Person" ADD "Pi" NUMBER(20,7) NOT NULL;
            """);
    }

    public override void AddForeignKeyOperation_without_principal_columns()
    {
        base.AddForeignKeyOperation_without_principal_columns();

        AssertSql(
            """
            ALTER TABLE "People" ADD FOREIGN KEY ("SpouseId") REFERENCES "People";
            """);
    }

    public override void AlterColumnOperation_without_column_type()
    {
        base.AlterColumnOperation_without_column_type();

        AssertSql(
            """
            ALTER TABLE "People" ALTER COLUMN "LuckyNumber" SET DATA TYPE NUMBER(10,0);
            """);
    }

    public override void RenameTableOperation_legacy()
    {
        // The legacy behavior is irrelevant; the provider didn't ship before EF Core 2.1
    }

    public override void RenameTableOperation()
    {
        base.RenameTableOperation();

        AssertSql(
            """
            ALTER TABLE "dbo"."People" RENAME TO "dbo"."Person";
            """);
    }

    public override void SqlOperation()
    {
        base.SqlOperation();

        AssertSql(
            """
            -- I <3 DDL
            """);
    }

    public override void InsertDataOperation_all_args_spatial()
    {
        // Spatial types are not supported
    }

    protected override string GetGeometryCollectionStoreType()
        => throw new UnreachableException();

    public override void InsertDataOperation_required_args()
    {
        base.InsertDataOperation_required_args();

        AssertSql(
            """
            INSERT INTO "dbo"."People" ("First Name")
            SELECT 'John';
            """);
    }

    public override void InsertDataOperation_required_args_composite()
    {
        base.InsertDataOperation_required_args_composite();

        AssertSql(
            """
            INSERT INTO "dbo"."People" ("First Name", "Last Name")
            SELECT 'John', 'Snow';
            """);
    }

    public override void InsertDataOperation_required_args_multiple_rows()
    {
        base.InsertDataOperation_required_args_multiple_rows();

        AssertSql(
            """
            INSERT INTO "dbo"."People" ("First Name")
            SELECT 'John';
            GO

            INSERT INTO "dbo"."People" ("First Name")
            SELECT 'Daenerys';
            """);
    }

    public override void InsertDataOperation_throws_for_unsupported_column_types()
        => base.InsertDataOperation_throws_for_unsupported_column_types();

    public override void DeleteDataOperation_all_args()
    {
        base.DeleteDataOperation_all_args();

        AssertSql(
            """
            DELETE FROM "People"
            WHERE "First Name" = 'Hodor';
            DELETE FROM "People"
            WHERE "First Name" = 'Daenerys';
            DELETE FROM "People"
            WHERE "First Name" = 'John';
            DELETE FROM "People"
            WHERE "First Name" = 'Arya';
            DELETE FROM "People"
            WHERE "First Name" = 'Harry';
            """);
    }

    public override void DeleteDataOperation_all_args_composite()
    {
        base.DeleteDataOperation_all_args_composite();

        AssertSql(
            """
            DELETE FROM "People"
            WHERE "First Name" = 'Hodor' AND "Last Name" IS NULL;
            DELETE FROM "People"
            WHERE "First Name" = 'Daenerys' AND "Last Name" = 'Targaryen';
            DELETE FROM "People"
            WHERE "First Name" = 'John' AND "Last Name" = 'Snow';
            DELETE FROM "People"
            WHERE "First Name" = 'Arya' AND "Last Name" = 'Stark';
            DELETE FROM "People"
            WHERE "First Name" = 'Harry' AND "Last Name" = 'Strickland';
            """);
    }

    public override void DeleteDataOperation_required_args()
    {
        base.DeleteDataOperation_required_args();

        AssertSql(
            """
            DELETE FROM "People"
            WHERE "Last Name" = 'Snow';
            """);
    }

    public override void DeleteDataOperation_required_args_composite()
    {
        base.DeleteDataOperation_required_args_composite();

        AssertSql(
            """
            DELETE FROM "People"
            WHERE "First Name" = 'John' AND "Last Name" = 'Snow';
            """);
    }

    public override void UpdateDataOperation_all_args()
    {
        base.UpdateDataOperation_all_args();

        AssertSql(
            """
            UPDATE "People" SET "Birthplace" = 'Winterfell', "House Allegiance" = 'Stark', "Culture" = 'Northmen'
            WHERE "First Name" = 'Hodor';
            UPDATE "People" SET "Birthplace" = 'Dragonstone', "House Allegiance" = 'Targaryen', "Culture" = 'Valyrian'
            WHERE "First Name" = 'Daenerys';
            """);
    }

    public override void UpdateDataOperation_all_args_composite()
    {
        base.UpdateDataOperation_all_args_composite();

        AssertSql(
            """
            UPDATE "People" SET "House Allegiance" = 'Stark'
            WHERE "First Name" = 'Hodor' AND "Last Name" IS NULL;
            UPDATE "People" SET "House Allegiance" = 'Targaryen'
            WHERE "First Name" = 'Daenerys' AND "Last Name" = 'Targaryen';
            """);
    }

    public override void UpdateDataOperation_all_args_composite_multi()
    {
        base.UpdateDataOperation_all_args_composite_multi();

        AssertSql(
            """
            UPDATE "People" SET "Birthplace" = 'Winterfell', "House Allegiance" = 'Stark', "Culture" = 'Northmen'
            WHERE "First Name" = 'Hodor' AND "Last Name" IS NULL;
            UPDATE "People" SET "Birthplace" = 'Dragonstone', "House Allegiance" = 'Targaryen', "Culture" = 'Valyrian'
            WHERE "First Name" = 'Daenerys' AND "Last Name" = 'Targaryen';
            """);
    }

    public override void UpdateDataOperation_all_args_multi()
    {
        base.UpdateDataOperation_all_args_multi();

        AssertSql(
            """
            UPDATE "People" SET "Birthplace" = 'Dragonstone', "House Allegiance" = 'Targaryen', "Culture" = 'Valyrian'
            WHERE "First Name" = 'Daenerys';
            """);
    }

    public override void UpdateDataOperation_required_args()
    {
        base.UpdateDataOperation_required_args();

        AssertSql(
            """
            UPDATE "People" SET "House Allegiance" = 'Targaryen'
            WHERE "First Name" = 'Daenerys';
            """);
    }

    public override void UpdateDataOperation_required_args_multiple_rows()
    {
        base.UpdateDataOperation_required_args_multiple_rows();

        AssertSql(
            """
            UPDATE "People" SET "House Allegiance" = 'Stark'
            WHERE "First Name" = 'Hodor';
            UPDATE "People" SET "House Allegiance" = 'Targaryen'
            WHERE "First Name" = 'Daenerys';
            """);
    }

    public override void UpdateDataOperation_required_args_composite()
    {
        base.UpdateDataOperation_required_args_composite();

        AssertSql(
            """
            UPDATE "People" SET "House Allegiance" = 'Targaryen'
            WHERE "First Name" = 'Daenerys' AND "Last Name" = 'Targaryen';
            """);
    }

    public override void UpdateDataOperation_required_args_composite_multi()
    {
        base.UpdateDataOperation_required_args_composite_multi();

        AssertSql(
            """
            UPDATE "People" SET "Birthplace" = 'Dragonstone', "House Allegiance" = 'Targaryen', "Culture" = 'Valyrian'
            WHERE "First Name" = 'Daenerys' AND "Last Name" = 'Targaryen';
            """);
    }

    public override void UpdateDataOperation_required_args_multi()
    {
        base.UpdateDataOperation_required_args_multi();

        AssertSql(
            """
            UPDATE "People" SET "Birthplace" = 'Dragonstone', "House Allegiance" = 'Targaryen', "Culture" = 'Valyrian'
            WHERE "First Name" = 'Daenerys';
            """);
    }

    public override void DefaultValue_with_line_breaks(bool isUnicode)
    {
        base.DefaultValue_with_line_breaks(isUnicode);

        AssertSql(
            """
            CREATE TABLE "dbo"."TestLineBreaks" (
                "TestDefaultValue" VARCHAR(16777216) NOT NULL DEFAULT '\
            \
            Various Line\
            Breaks\
            '
            );
            """);
    }

    public override void DefaultValue_with_line_breaks_2(bool isUnicode)
    {
        base.DefaultValue_with_line_breaks_2(isUnicode);

        var escapedDefaultValue = string.Concat(Enumerable.Range(0, 300).Select(i => i + "\\\r\\\n"));

        AssertSql(
            $"""
            CREATE TABLE "dbo"."TestLineBreaks" (
                "TestDefaultValue" VARCHAR(16777216) NOT NULL DEFAULT '{escapedDefaultValue}'
            );
            """);
    }

    public override void Sequence_restart_operation(long? startsAt)
    {
        var ex = Assert.Throws<NotSupportedException>(() => base.Sequence_restart_operation(startsAt));

        Assert.Equal("Sequence restarting is not supported in Snowflake", ex.Message);
    }

    [ConditionalFact]
    public virtual void SnowflakeCreateDatabaseOperation_required_args()
    {
        Generate(
            new SnowflakeCreateDatabaseOperation
            {
                Name = "SNOWPACK"
            });

        AssertSql(
            """
            CREATE DATABASE "SNOWPACK";
            """);
    }

    [ConditionalFact]
    public virtual void SnowflakeCreateDatabaseOperation_all_args()
    {
        Generate(
            new SnowflakeCreateDatabaseOperation
            {
                Name = "POKRYWA_ŚNIEŻNA",
                Collation = "pl_pl"
            });

        AssertSql(
            """
            CREATE DATABASE "POKRYWA_ŚNIEŻNA"
            DEFAULT_DDL_COLLATION 'pl_pl';
            """);
    }

    [ConditionalFact]
    public virtual void SnowflakeDropDatabaseOperation_required_args()
    {
        Generate(
            new SnowflakeDropDatabaseOperation
            {
                Name = "SNOWPACK"
            });

        AssertSql(
            """
            DROP DATABASE "SNOWPACK";
            """);
    }

    [ConditionalFact]
    public virtual void SnowflakeDropViewOperation_required_args()
    {
        Generate(
            new SnowflakeDropViewOperation
            {
                Name = "SNOW_BIRD"
            });

        AssertSql(
            """
            DROP VIEW "SNOW_BIRD";
            """);
    }

    [ConditionalFact]
    public virtual void SnowflakeDropViewOperation_all_args()
    {
        Generate(
            new SnowflakeDropViewOperation
            {
                Schema = "SNOW",
                Name = "SNOW_BIRD"
            });

        AssertSql(
            """
            DROP VIEW "SNOW"."SNOW_BIRD";
            """);
    }

    [ConditionalFact]
    public virtual void EnsureSchemaOperation_required_args()
    {
        Generate(
            new EnsureSchemaOperation
            {
                Name = "SNOW"
            });

        AssertSql(
            """
            CREATE SCHEMA IF NOT EXISTS "SNOW";
            """);
    }

    [ConditionalFact]
    public virtual void CreateSequenceOperation_required_args()
    {
        Generate(
            new CreateSequenceOperation
            {
                Name = "Flakes",
                [SnowflakeAnnotationNames.SequenceIsOrdered] = false
            });

        AssertSql(
            """
            CREATE SEQUENCE "Flakes" START WITH 1 INCREMENT BY 1 NOORDER;
            """);
    }

    [ConditionalFact]
    public virtual void CreateSequenceOperation_with_type()
    {
        Generate(
            new CreateSequenceOperation
            {
                Name = "Flakes",
                ClrType = typeof(int),
                StartValue = 3,
                IncrementBy = 2,
                [SnowflakeAnnotationNames.SequenceIsOrdered] = true
            });

        AssertSql(
            """
            CREATE SEQUENCE "Flakes" START WITH 3 INCREMENT BY 2 ORDER;
            """);
    }

    [ConditionalFact]
    public virtual void RenameSequenceOperation_required_args()
    {
        Generate(
            new RenameSequenceOperation
            {
                Name = "Flakes",
                NewName = "Snowflakes"
            });

        AssertSql(
            """
            ALTER SEQUENCE "Flakes" RENAME TO "Snowflakes";
            """);
    }

    [ConditionalFact]
    public virtual void RenameSequenceOperation_all_args()
    {
        Generate(
            new RenameSequenceOperation
            {
                Schema = "Arctic",
                Name = "Flakes",
                NewSchema = "Antarctic",
                NewName = "Snowflakes"
            });

        AssertSql(
            """
            ALTER SEQUENCE "Arctic"."Flakes" RENAME TO "Antarctic"."Snowflakes";
            """);
    }

    [ConditionalFact]
    public virtual void AlterSequenceOperation_required_args()
    {
        Generate(
            new AlterSequenceOperation
            {
                Name = "Flakes",
                [SnowflakeAnnotationNames.SequenceIsOrdered] = true,
                OldSequence = new CreateSequenceOperation
                {
                    [SnowflakeAnnotationNames.SequenceIsOrdered] = true
                }
            });

        Assert.Empty(Sql);
    }

    [ConditionalFact]
    public virtual void AlterSequenceOperation_all_args()
    {
        Generate(
            new AlterSequenceOperation
            {
                Schema = "Arctic",
                Name = "Flakes",
                IncrementBy = 2,
                [SnowflakeAnnotationNames.SequenceIsOrdered] = false,
                OldSequence = new CreateSequenceOperation
                {
                    [SnowflakeAnnotationNames.SequenceIsOrdered] = true
                }
            });

        AssertSql(
            """
            ALTER SEQUENCE "Arctic"."Flakes" SET INCREMENT BY 2;
            GO

            ALTER SEQUENCE "Arctic"."Flakes" SET NOORDER;
            """);
    }

    [ConditionalFact]
    public virtual void CreateTableOperation_required_args()
    {
        Generate(
            new CreateTableOperation
            {
                Name = "Snowmen",
                Columns =
                {
                    new AddColumnOperation
                    {
                        Table = "Snowmen",
                        Name = "Id",
                        ClrType = typeof(int)
                    }
                }
            });

        AssertSql(
            """
            CREATE TABLE "Snowmen" (
                "Id" NUMBER(10,0) NOT NULL
            );
            """);
    }

    [ConditionalFact]
    public virtual void CreateTableOperation_all_args()
    {
        Generate(
            new CreateTableOperation
            {
                Schema = "Arctic",
                Name = "Snowmen",
                Comment = "Frosty and friends",
                [SnowflakeAnnotationNames.TableType] = SnowflakeTableType.Transient,
                Columns =
                {
                    new AddColumnOperation
                    {
                        Schema = "Arctic",
                        Table = "Snowmen",
                        Name = "Id",
                        ClrType = typeof(int)
                    }
                }
            });

        AssertSql(
            """
            CREATE TRANSIENT TABLE "Arctic"."Snowmen" (
                "Id" NUMBER(10,0) NOT NULL
            ) COMMENT = 'Frosty and friends';
            """);
    }

    [ConditionalFact]
    public virtual void CreateTableOperation_hybrid()
    {
        Generate(
            new CreateTableOperation
            {
                Name = "Snowmen",
                [SnowflakeAnnotationNames.TableType] = SnowflakeTableType.Hybrid,
                Columns =
                {
                    new AddColumnOperation
                    {
                        Table = "Snowmen",
                        Name = "Id",
                        ClrType = typeof(int)
                    }
                },
                PrimaryKey = new AddPrimaryKeyOperation
                {
                    Table = "Snowmen",
                    Name = "PK_Snowmen",
                    Columns = ["Id"]
                }
            });

        AssertSql(
            """
            CREATE HYBRID TABLE "Snowmen" (
                "Id" NUMBER(10,0) NOT NULL,
                CONSTRAINT "PK_Snowmen" PRIMARY KEY ("Id")
            );
            """);
    }

    [ConditionalFact]
    public virtual void CreateTableOperation_throws_when_check_constraint()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => Generate(
                new CreateTableOperation
                {
                    Name = "Snowmen",
                    Columns =
                    {
                        new AddColumnOperation
                        {
                            Table = "Snowmen",
                            Name = "Height",
                            ClrType = typeof(int)
                        }
                    },
                    CheckConstraints =
                    {
                        new AddCheckConstraintOperation
                        {
                            Table = "Snowmen",
                            Name = "CK_Snowmen_Height",
                            Sql = "\"Height\" > 0"
                        }
                    }
                }));

        Assert.Equal("Snowflake does not support check constraints", ex.Message);
    }

    [ConditionalFact]
    public virtual void AlterTableOperation_required_args()
    {
        Generate(
            new AlterTableOperation
            {
                Name = "Snowmen"
            });

        Assert.Empty(Sql);
    }

    [ConditionalFact]
    public virtual void AlterTableOperation_all_args()
    {
        Generate(
            new AlterTableOperation
            {
                Schema = "Arctic",
                Name = "Snowmen",
                Comment = "Frosty and friends"
            });

        AssertSql(
            """
            ALTER TABLE "Arctic"."Snowmen" SET COMMENT = 'Frosty and friends';
            """);
    }

    [ConditionalFact]
    public virtual void AlterTableOperation_comment_removed()
    {
        Generate(
            new AlterTableOperation
            {
                Name = "Snowmen",
                OldTable = new CreateTableOperation
                {
                    Comment = "Frosty and friends"
                }
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" UNSET COMMENT;
            """);
    }

    [ConditionalFact]
    public virtual void AlterTableOperation_throws_when_table_type_changed()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => Generate(
                new AlterTableOperation
                {
                    Name = "Snowmen",
                    [SnowflakeAnnotationNames.TableType] = SnowflakeTableType.Transient,
                    OldTable = new CreateTableOperation
                    {
                        [SnowflakeAnnotationNames.TableType] = SnowflakeTableType.Permanent
                    }
                }));

        Assert.Equal("To change the table type, the table needs to be dropped and recreated.", ex.Message);
    }

    [ConditionalFact]
    public virtual void AddCheckConstraintOperation_throws()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => Generate(
                new AddCheckConstraintOperation
                {
                    Table = "Snowmen",
                    Name = "CK_Snowmen_Height",
                    Sql = "\"Height\" > 0"
                }));

        Assert.Equal("Snowflake does not support check constraints", ex.Message);
    }

    [ConditionalFact]
    public virtual void DropCheckConstraintOperation_throws()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => Generate(
                new DropCheckConstraintOperation
                {
                    Table = "Snowmen",
                    Name = "CK_Snowmen_Height"
                }));

        Assert.Equal("Snowflake does not support check constraints", ex.Message);
    }

    [ConditionalFact]
    public virtual void CreateIndexOperation_required_args()
    {
        Generate(
            new CreateIndexOperation
            {
                Table = "Snowmen",
                Name = "IX_Snowmen_Name",
                Columns = ["Name"]
            });

        Assert.Empty(Sql);
    }

    [ConditionalFact]
    public virtual void CreateIndexOperation_throws_when_index_behavior_disallow()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => Generate(
                b => b.SetIndexBehavior(SnowflakeIndexBehavior.Disallow),
                new CreateIndexOperation
                {
                    Table = "Snowmen",
                    Name = "IX_Snowmen_Name",
                    Columns = ["Name"]
                }));

        Assert.Equal(
            "Index behavior is set to Disallow, any index operations are blocked since Snowflake doesn't support indexes. If you want to ignore all index definitions set call HasIndexBehavior(SnowflakeIndexBehavior.Ignore) on model",
            ex.Message);
    }

    [ConditionalFact]
    public virtual void RenameIndexOperation_required_args()
    {
        Generate(
            new RenameIndexOperation
            {
                Table = "Snowmen",
                Name = "IX_Snowmen_Name",
                NewName = "IX_Snowmen_Nickname"
            });

        Assert.Empty(Sql);
    }

    [ConditionalFact]
    public virtual void DropIndexOperation_required_args()
    {
        Generate(
            new DropIndexOperation
            {
                Table = "Snowmen",
                Name = "IX_Snowmen_Name"
            });

        Assert.Empty(Sql);
    }

    [ConditionalFact]
    public virtual void AddColumnOperation_required_args()
    {
        Generate(
            new AddColumnOperation
            {
                Table = "Snowmen",
                Name = "Height",
                ClrType = typeof(int)
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" ADD "Height" NUMBER(10,0) NOT NULL;
            """);
    }

    [ConditionalFact]
    public virtual void AddColumnOperation_all_args()
    {
        Generate(
            new AddColumnOperation
            {
                Schema = "Arctic",
                Table = "Snowmen",
                Name = "Name",
                ClrType = typeof(string),
                ColumnType = "VARCHAR",
                IsNullable = true,
                Collation = "pl_pl",
                DefaultValue = "Bałwan",
                Comment = "Given by the children who built it"
            });

        AssertSql(
            """
            ALTER TABLE "Arctic"."Snowmen" ADD "Name" VARCHAR COLLATE 'pl_pl' NULL DEFAULT 'Bałwan' COMMENT 'Given by the children who built it';
            """);
    }

    [ConditionalFact]
    public virtual void AddColumnOperation_with_identity()
    {
        Generate(
            new AddColumnOperation
            {
                Table = "Snowmen",
                Name = "Id",
                ClrType = typeof(long),
                ColumnType = "NUMBER(19,0)",
                [SnowflakeAnnotationNames.Identity] = "START 1 INCREMENT 1"
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" ADD "Id" NUMBER(19,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER;
            """);
    }

    [ConditionalFact]
    public virtual void AddColumnOperation_with_semi_structured_default_value()
    {
        Generate(
            new AddColumnOperation
            {
                Table = "Snowmen",
                Name = "Accessories",
                ClrType = typeof(string),
                ColumnType = "VARIANT",
                DefaultValue = """{"hat":"top","nose":"carrot"}"""
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" ADD "Accessories" VARIANT NOT NULL DEFAULT '{\"hat\":\"top\",\"nose\":\"carrot\"}';
            """);
    }

    [ConditionalFact]
    public virtual void AddColumnOperation_with_value_generation_strategy()
    {
        Generate(
            new AddColumnOperation
            {
                Table = "Snowmen",
                Name = "Id",
                ClrType = typeof(long),
                ColumnType = "NUMBER(19,0)",
                [SnowflakeAnnotationNames.ValueGenerationStrategy] = SnowflakeValueGenerationStrategy.AutoIncrement
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" ADD "Id" NUMBER(19,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER;
            """);
    }

    [ConditionalFact]
    public virtual void AddColumnOperation_with_identity_order()
    {
        Generate(
            new AddColumnOperation
            {
                Table = "Snowmen",
                Name = "Id",
                ClrType = typeof(long),
                ColumnType = "NUMBER(19,0)",
                [SnowflakeAnnotationNames.Identity] = "START 1 INCREMENT 1 NOORDER"
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" ADD "Id" NUMBER(19,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER;
            """);
    }

    [ConditionalFact]
    public virtual void AddColumnOperation_with_default_value_sql()
    {
        Generate(
            new AddColumnOperation
            {
                Table = "Snowmen",
                Name = "Temperature",
                ClrType = typeof(int),
                ColumnType = "NUMBER(10,0)",
                DefaultValueSql = "-5"
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" ADD "Temperature" NUMBER(10,0) NOT NULL DEFAULT (-5);
            """);
    }

    [ConditionalFact]
    public virtual void AddColumnOperation_computed()
    {
        Generate(
            new AddColumnOperation
            {
                Table = "Snowmen",
                Name = "Initial",
                ClrType = typeof(string),
                ColumnType = "VARCHAR",
                ComputedColumnSql = "LEFT(\"Name\", 1)"
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" ADD "Initial" VARCHAR AS (LEFT("Name", 1));
            """);
    }

    [ConditionalFact]
    public virtual void AddColumnOperation_computed_with_collation()
    {
        Generate(
            new AddColumnOperation
            {
                Table = "Snowmen",
                Name = "Initial",
                ClrType = typeof(string),
                ColumnType = "VARCHAR",
                Collation = "pl_pl",
                ComputedColumnSql = "LEFT(\"Name\", 1)"
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" ADD "Initial" VARCHAR COLLATE 'pl_pl' AS COLLATE(LEFT("Name", 1), 'pl_pl');
            """);
    }

    [ConditionalFact]
    public virtual void AddColumnOperation_throw_when_stored_calculated_column()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => Generate(
                new AddColumnOperation
                {
                    Table = "Snowmen",
                    Name = "Initial",
                    ClrType = typeof(string),
                    ColumnType = "VARCHAR",
                    ComputedColumnSql = "LEFT(\"Name\", 1)",
                    IsStored = true
                }));

        Assert.Equal(
            "Stored generated columns are not supported by Snowflake, specify 'stored: false' in 'HasComputedColumnSql' in your context's OnModelCreating.",
            ex.Message);
    }

    [ConditionalFact]
    public virtual void AlterColumnOperation_required_args()
    {
        Generate(
            new AlterColumnOperation
            {
                Table = "Snowmen",
                Name = "Height",
                ClrType = typeof(int),
                ColumnType = "NUMBER(10,0)",
                OldColumn = new AddColumnOperation
                {
                    ClrType = typeof(int),
                    ColumnType = "NUMBER(10,0)"
                }
            });

        Assert.Empty(Sql);
    }

    [ConditionalFact]
    public virtual void AlterColumnOperation_all_args()
    {
        Generate(
            new AlterColumnOperation
            {
                Schema = "Arctic",
                Table = "Snowmen",
                Name = "Height",
                ClrType = typeof(long),
                ColumnType = "NUMBER(19,0)",
                DefaultValue = 180L,
                Comment = "In centimeters",
                OldColumn = new AddColumnOperation
                {
                    ClrType = typeof(int),
                    ColumnType = "NUMBER(10,0)",
                    IsNullable = true,
                    [SnowflakeAnnotationNames.Identity] = "START 1 INCREMENT 1"
                }
            });

        AssertSql(
            """
            ALTER TABLE "Arctic"."Snowmen" ALTER COLUMN "Height" DROP DEFAULT;
            GO

            UPDATE "Arctic"."Snowmen" SET "Height" = 180 WHERE "Height" IS NULL;
            GO

            ALTER TABLE "Arctic"."Snowmen" ALTER COLUMN "Height" SET NOT NULL;
            GO

            ALTER TABLE "Arctic"."Snowmen" ALTER COLUMN "Height" SET DATA TYPE NUMBER(19,0);
            GO

            ALTER TABLE "Arctic"."Snowmen" ALTER COLUMN "Height" COMMENT 'In centimeters';
            """);
    }

    [ConditionalFact]
    public virtual void AlterColumnOperation_computed_column_sql_changed()
    {
        Generate(
            new AlterColumnOperation
            {
                Table = "Snowmen",
                Name = "Initial",
                ClrType = typeof(string),
                ColumnType = "VARCHAR(1)",
                ComputedColumnSql = "LEFT(\"Nickname\", 1)",
                Comment = "First letter",
                OldColumn = new AddColumnOperation
                {
                    ClrType = typeof(string),
                    ColumnType = "VARCHAR(1)",
                    ComputedColumnSql = "LEFT(\"Name\", 1)",
                    Comment = "First letter"
                }
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" DROP COLUMN "Initial";
            GO

            ALTER TABLE "Snowmen" ADD "Initial" VARCHAR(1) AS (LEFT("Nickname", 1)) COMMENT 'First letter';
            """);
    }

    [ConditionalFact]
    public virtual void AlterColumnOperation_nullable_changed_with_default_value_sql()
    {
        Generate(
            new AlterColumnOperation
            {
                Table = "Snowmen",
                Name = "Height",
                ClrType = typeof(int),
                ColumnType = "NUMBER(10,0)",
                DefaultValueSql = "100",
                OldColumn = new AddColumnOperation
                {
                    ClrType = typeof(int),
                    ColumnType = "NUMBER(10,0)",
                    IsNullable = true
                }
            });

        AssertSql(
            """
            UPDATE "Snowmen" SET "Height" = 100 WHERE "Height" IS NULL;
            GO

            ALTER TABLE "Snowmen" ALTER COLUMN "Height" SET NOT NULL;
            """);
    }

    [ConditionalFact]
    public virtual void AlterColumnOperation_not_null_and_comment_removed()
    {
        Generate(
            new AlterColumnOperation
            {
                Table = "Snowmen",
                Name = "Height",
                ClrType = typeof(int),
                ColumnType = "NUMBER(10,0)",
                IsNullable = true,
                OldColumn = new AddColumnOperation
                {
                    ClrType = typeof(int),
                    ColumnType = "NUMBER(10,0)",
                    Comment = "In centimeters"
                }
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" ALTER COLUMN "Height" DROP NOT NULL;
            GO

            ALTER TABLE "Snowmen" ALTER COLUMN "Height" UNSET COMMENT;
            """);
    }

    [ConditionalFact]
    public virtual void AlterColumnOperation_throws_when_stored_calculated_column()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => Generate(
                new AlterColumnOperation
                {
                    Table = "Snowmen",
                    Name = "Initial",
                    ClrType = typeof(string),
                    ColumnType = "VARCHAR(1)",
                    ComputedColumnSql = "LEFT(\"Name\", 1)",
                    IsStored = true,
                    OldColumn = new AddColumnOperation
                    {
                        ClrType = typeof(string),
                        ColumnType = "VARCHAR(1)",
                        ComputedColumnSql = "LEFT(\"Name\", 1)"
                    }
                }));

        Assert.Equal(
            "Stored generated columns are not supported by Snowflake, specify 'stored: false' in 'HasComputedColumnSql' in your context's OnModelCreating.",
            ex.Message);
    }

    [ConditionalFact]
    public virtual void AlterColumnOperation_throws_when_collation_changed()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => Generate(
                new AlterColumnOperation
                {
                    Table = "Snowmen",
                    Name = "Name",
                    ClrType = typeof(string),
                    ColumnType = "VARCHAR",
                    Collation = "pl_pl",
                    OldColumn = new AddColumnOperation
                    {
                        ClrType = typeof(string),
                        ColumnType = "VARCHAR"
                    }
                }));

        Assert.Equal(
            "Collation change is not supported in Snowflake. Column has to be dropped and recreated with different collation",
            ex.Message);
    }

    [ConditionalFact]
    public virtual void AlterColumnOperation_throws_when_autoIncrement_added()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => Generate(
                new AlterColumnOperation
                {
                    Table = "Snowmen",
                    Name = "Id",
                    ClrType = typeof(long),
                    ColumnType = "NUMBER(19,0)",
                    [SnowflakeAnnotationNames.ValueGenerationStrategy] = SnowflakeValueGenerationStrategy.AutoIncrement,
                    OldColumn = new AddColumnOperation
                    {
                        ClrType = typeof(long),
                        ColumnType = "NUMBER(19,0)"
                    }
                }));

        Assert.Equal("To add AutoIncrement property to a column, the column needs to be dropped and recreated.", ex.Message);
    }

    [ConditionalFact]
    public virtual void RenameColumnOperation_required_args()
    {
        Generate(
            new RenameColumnOperation
            {
                Table = "Snowmen",
                Name = "Nose",
                NewName = "Carrot"
            });

        AssertSql(
            """
            ALTER TABLE "Snowmen" RENAME COLUMN "Nose" TO "Carrot";
            """);
    }
}

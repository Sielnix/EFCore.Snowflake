using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EFCore.Snowflake.FunctionalTests;
public class CustomConvertersSnowflakeTest : CustomConvertersTestBase<CustomConvertersSnowflakeTest.CustomConvertersSnowflakeFixture>
{
    public CustomConvertersSnowflakeTest(CustomConvertersSnowflakeFixture fixture)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }

    // Disabled: Snowflake is case-sensitive
    public override Task Can_insert_and_read_back_with_case_insensitive_string_key() => Task.CompletedTask;

    public override void Value_conversion_on_enum_collection_contains()
        => Assert.Contains(
            CoreStrings.TranslationFailed("").Substring(47),
            Assert.Throws<InvalidOperationException>(() => base.Value_conversion_on_enum_collection_contains()).Message);
    
    public override async Task Can_query_with_null_parameters_using_any_nullable_data_type()
    {
        using (var context = CreateContext())
        {
            context.Set<BuiltInNullableDataTypes>().Add(
                new BuiltInNullableDataTypes { Id = 711 });

            Assert.Equal(1, await context.SaveChangesAsync());
        }

        using (var context = CreateContext())
        {
            var entity = (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711).ToListAsync()).Single();

            short? param1 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableInt16 == param1).ToListAsync())
                .Single());
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && (long?)e.TestNullableInt16 == param1)
                    .ToListAsync())
                .Single());

            int? param2 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableInt32 == param2).ToListAsync())
                .Single());

            long? param3 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableInt64 == param3).ToListAsync())
                .Single());

            double? param4 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableDouble == param4).ToListAsync())
                .Single());

            decimal? param5 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableDecimal == param5).ToListAsync())
                .Single());

            DateTime? param6 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableDateTime == param6).ToListAsync())
                .Single());

            // ONLY CHANGE: no datetimeoffset check because of snowflake .net connector bug
            //DateTimeOffset? param7 = null;
            //Assert.Same(
            //    entity,
            //    (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableDateTimeOffset == param7)
            //        .ToListAsync())
            //    .Single());

            TimeSpan? param8 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableTimeSpan == param8).ToListAsync())
                .Single());

            DateOnly? param9 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableDateOnly == param9).ToListAsync())
                .Single());

            TimeOnly? param10 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableTimeOnly == param10).ToListAsync())
                .Single());

            float? param11 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableSingle == param11).ToListAsync())
                .Single());

            bool? param12 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableBoolean == param12).ToListAsync())
                .Single());

            byte? param13 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableByte == param13).ToListAsync())
                .Single());

            Enum64? param14 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.Enum64 == param14).ToListAsync()).Single());

            Enum32? param15 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.Enum32 == param15).ToListAsync()).Single());

            Enum16? param16 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.Enum16 == param16).ToListAsync()).Single());

            Enum8? param17 = null;
            Assert.Same(
                entity,
                (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.Enum8 == param17).ToListAsync()).Single());

            var entityType = context.Model.FindEntityType(typeof(BuiltInNullableDataTypes))!;
            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.TestNullableUnsignedInt16)) != null)
            {
                ushort? param18 = null;
                Assert.Same(
                    entity,
                    (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableUnsignedInt16 == param18)
                        .ToListAsync())
                    .Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.TestNullableUnsignedInt32)) != null)
            {
                uint? param19 = null;
                Assert.Same(
                    entity,
                    (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableUnsignedInt32 == param19)
                        .ToListAsync())
                    .Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.TestNullableUnsignedInt64)) != null)
            {
                ulong? param20 = null;
                Assert.Same(
                    entity,
                    (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableUnsignedInt64 == param20)
                        .ToListAsync())
                    .Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.TestNullableCharacter)) != null)
            {
                char? param21 = null;
                Assert.Same(
                    entity,
                    (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableCharacter == param21)
                        .ToListAsync())
                    .Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.TestNullableSignedByte)) != null)
            {
                sbyte? param22 = null;
                Assert.Same(
                    entity,
                    (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableSignedByte == param22)
                        .ToListAsync())
                    .Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.EnumU64)) != null)
            {
                EnumU64? param23 = null;
                Assert.Same(
                    entity,
                    (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.EnumU64 == param23).ToListAsync()).Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.EnumU32)) != null)
            {
                EnumU32? param24 = null;
                Assert.Same(
                    entity,
                    (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.EnumU32 == param24).ToListAsync()).Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.EnumU16)) != null)
            {
                EnumU16? param25 = null;
                Assert.Same(
                    entity,
                    (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.EnumU16 == param25).ToListAsync()).Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.EnumS8)) != null)
            {
                EnumS8? param26 = null;
                Assert.Same(
                    entity,
                    (await context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.EnumS8 == param26).ToListAsync()).Single());
            }
        }
    }


    public class CustomConvertersSnowflakeFixture : CustomConvertersFixtureBase
    {
        public override bool StrictEquality
            => true;

        public override bool SupportsAnsi
            => false;

        public override bool SupportsUnicodeToAnsiConversion
            => false;

        public override bool SupportsLargeStringComparisons
            => true;

        protected override ITestStoreFactory TestStoreFactory
            => SnowflakeTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;

        public override bool SupportsBinaryKeys
            => true;

        public override bool SupportsDecimalComparisons
            => true;

        public override DateTime DefaultDateTime
            => new();

        public override bool PreservesDateTimeKind
            => false;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            modelBuilder.Entity<Person>(
                b =>
                {
                    IMutableIndex index = b.HasIndex(p => p.SSN).IsUnique().Metadata;

                    b.Metadata.RemoveIndex(index.Properties);
                });

            // todo: Remove exclusions below after fixing datetimeoffset bug in snowflake .net connector
            modelBuilder.Entity<BuiltInDataTypes>(
                b =>
                {
                    b.Ignore(dt => dt.TestDateTimeOffset);
                });

            modelBuilder.Entity<BuiltInNullableDataTypes>(
                b =>
                {
                    b.Ignore(dt => dt.TestNullableDateTimeOffset);
                });

            modelBuilder.Entity<BuiltInDataTypesShadow>(
                b =>
                {
                    b.Ignore(nameof(BuiltInDataTypes.TestDateTimeOffset));
                });

            modelBuilder.Entity<BuiltInNullableDataTypesShadow>(
                b =>
                {
                    b.Ignore(nameof(BuiltInNullableDataTypes.TestNullableDateTimeOffset));
                });
        }
    }
}

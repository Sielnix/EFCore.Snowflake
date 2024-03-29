using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using EFCore.Snowflake.FunctionalTests.TestUtilities;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace EFCore.Snowflake.FunctionalTests;

public class BuiltInDataTypesSnowflakeTest : BuiltInDataTypesTestBase<BuiltInDataTypesSnowflakeTest.BuiltInDataTypesSnowflakeFixture>
{
    public BuiltInDataTypesSnowflakeTest(BuiltInDataTypesSnowflakeFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override void Can_query_with_null_parameters_using_any_nullable_data_type()
    {
        using (var context = CreateContext())
        {
            context.Set<BuiltInNullableDataTypes>().Add(
                new BuiltInNullableDataTypes { Id = 711 });

            Assert.Equal(1, context.SaveChanges());
        }

        using (var context = CreateContext())
        {
            var entity = context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711).ToList().Single();

            short? param1 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableInt16 == param1).ToList().Single());
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && (long?)e.TestNullableInt16 == param1).ToList()
                    .Single());

            int? param2 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableInt32 == param2).ToList().Single());

            long? param3 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableInt64 == param3).ToList().Single());

            double? param4 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableDouble == param4).ToList().Single());

            decimal? param5 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableDecimal == param5).ToList().Single());

            DateTime? param6 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableDateTime == param6).ToList().Single());

            // ONLY CHANGE: no datetimeoffset check because of snowflake .net connector bug
            //DateTimeOffset? param7 = null;
            //Assert.Same(
            //    entity,
            //    context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableDateTimeOffset == param7).ToList()
            //        .Single());

            TimeSpan? param8 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableTimeSpan == param8).ToList().Single());

            DateOnly? param9 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableDateOnly == param9).ToList().Single());

            TimeOnly? param10 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableTimeOnly == param10).ToList().Single());

            float? param11 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableSingle == param11).ToList().Single());

            bool? param12 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableBoolean == param12).ToList().Single());

            byte? param13 = null;
            Assert.Same(
                entity,
                context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableByte == param13).ToList().Single());

            Enum64? param14 = null;
            Assert.Same(
                entity, context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.Enum64 == param14).ToList().Single());

            Enum32? param15 = null;
            Assert.Same(
                entity, context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.Enum32 == param15).ToList().Single());

            Enum16? param16 = null;
            Assert.Same(
                entity, context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.Enum16 == param16).ToList().Single());

            Enum8? param17 = null;
            Assert.Same(
                entity, context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.Enum8 == param17).ToList().Single());

            var entityType = context.Model.FindEntityType(typeof(BuiltInNullableDataTypes));
            if (entityType!.FindProperty(nameof(BuiltInNullableDataTypes.TestNullableUnsignedInt16)) != null)
            {
                ushort? param18 = null;
                Assert.Same(
                    entity,
                    context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableUnsignedInt16 == param18).ToList()
                        .Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.TestNullableUnsignedInt32)) != null)
            {
                uint? param19 = null;
                Assert.Same(
                    entity,
                    context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableUnsignedInt32 == param19).ToList()
                        .Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.TestNullableUnsignedInt64)) != null)
            {
                ulong? param20 = null;
                Assert.Same(
                    entity,
                    context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableUnsignedInt64 == param20).ToList()
                        .Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.TestNullableCharacter)) != null)
            {
                char? param21 = null;
                Assert.Same(
                    entity,
                    context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableCharacter == param21).ToList()
                        .Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.TestNullableSignedByte)) != null)
            {
                sbyte? param22 = null;
                Assert.Same(
                    entity,
                    context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.TestNullableSignedByte == param22).ToList()
                        .Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.EnumU64)) != null)
            {
                EnumU64? param23 = null;
                Assert.Same(
                    entity, context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.EnumU64 == param23).ToList().Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.EnumU32)) != null)
            {
                EnumU32? param24 = null;
                Assert.Same(
                    entity, context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.EnumU32 == param24).ToList().Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.EnumU16)) != null)
            {
                EnumU16? param25 = null;
                Assert.Same(
                    entity, context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.EnumU16 == param25).ToList().Single());
            }

            if (entityType.FindProperty(nameof(BuiltInNullableDataTypes.EnumS8)) != null)
            {
                EnumS8? param26 = null;
                Assert.Same(
                    entity, context.Set<BuiltInNullableDataTypes>().Where(e => e.Id == 711 && e.EnumS8 == param26).ToList().Single());
            }
        }
    }

    public class BuiltInDataTypesSnowflakeFixture : BuiltInDataTypesFixtureBase
    {
        public override bool StrictEquality => false;

        public override bool SupportsAnsi => false;

        public override bool SupportsUnicodeToAnsiConversion => false;

        public override bool SupportsLargeStringComparisons => false;

        public override bool SupportsBinaryKeys => true;

        public override DateTime DefaultDateTime => new DateTime();

        public override bool PreservesDateTimeKind => false;

        public override bool SupportsDecimalComparisons => true;

        protected override ITestStoreFactory TestStoreFactory => SnowflakeTestStoreFactory.Instance;
        public TestSqlLoggerFactory TestSqlLoggerFactory => (TestSqlLoggerFactory)ServiceProvider.GetRequiredService<ILoggerFactory>();

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

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

using System;
using Machine.Specifications;

namespace Elders.Cronus.Projections.Cassandra.Tests
{
    public class When_parsing_version_from_column_family_with_less_than_3_parts
    {
        Establish context = () =>
        {
            columnFamily = $"projname_2";
        };

        Because of = () => ex = Catch.Exception(() => version = naming.Parse(columnFamily));

        It should_not_parse_version = () => version.ShouldBeNull();

        It should_throw = () => ex.ShouldNotBeNull();

        static VersionedProjectionsNaming naming = new VersionedProjectionsNaming();
        static ProjectionVersion version;
        static string columnFamily;
        static Exception ex;
    }
}

using Machine.Specifications;

namespace Elders.Cronus.Projections.Cassandra.Tests
{
    public class When_parsing_version_from_column_family
    {
        Establish context = () =>
        {
            projectionName = "projname";
            revision = 2;
            hash = "hash";
            columnFamily = $"{projectionName}_{revision}_{hash}";
        };

        Because of = () => version = naming.Parse(columnFamily);

        It should_parse_version = () => version.ShouldNotBeNull();

        It should_parse_projection_name = () => version.ProjectionName.ShouldEqual(projectionName);

        It should_parse_revision = () => version.Revision.ShouldEqual(revision);

        It should_parse_hash = () => version.Hash.ShouldEqual(hash);

        static VersionedProjectionsNaming naming = new VersionedProjectionsNaming();
        static ProjectionVersion version;
        static string columnFamily;
        static string projectionName;
        static int revision;
        static string hash;
    }
}

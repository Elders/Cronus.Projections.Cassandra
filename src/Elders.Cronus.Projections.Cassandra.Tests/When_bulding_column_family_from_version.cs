using Machine.Specifications;

namespace Elders.Cronus.Projections.Cassandra.Tests;

public class When_bulding_projection_column_family_from_version
{
    Establish context = () =>
    {
        version = new ProjectionVersion("projName", ProjectionStatus.Live, 2, "hash");
    };

    Because of = () => columnFamily = naming.GetColumnFamily(version);

    It should_create_column_family = () => columnFamily.ShouldEqual("projname_2_hash");

    static VersionedProjectionsNaming naming = new VersionedProjectionsNaming();
    static ProjectionVersion version;
    static string columnFamily;
}

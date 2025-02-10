using Machine.Specifications;

namespace Elders.Cronus.Projections.Cassandra.Tests;

internal class When_getting_column_family
{
    static VersionedProjectionsNaming _naming;

    static ProjectionVersion _regularVersion;
    static ProjectionVersion _versionWithDash;
    static ProjectionVersion _versionWithUpperCase;
    static ProjectionVersion _versionWithDashAndUpperCase;

    static string name;

    Establish context = () =>
    {
        _naming = new VersionedProjectionsNaming();
        _regularVersion = new ProjectionVersion("proj", ProjectionStatus.Live, 1, "hash1");
        _versionWithDash = new ProjectionVersion("proj-asd", ProjectionStatus.Live, 2, "hash2");
        _versionWithUpperCase = new ProjectionVersion("projALaBAla", ProjectionStatus.Live, 3, "hash3");
        _versionWithDashAndUpperCase = new ProjectionVersion("-projAla-Ba-L--a", ProjectionStatus.Live, 4, "hash4");
    };

    class And_version_name_is_only_lower_case
    {
        Because of = () => name = _naming.GetColumnFamily(_regularVersion);
        It should_construct_correctly = () => name.ShouldEqual($"proj_{_regularVersion.Revision}_{_regularVersion.Hash}");
    }

    class And_version_name_contains_dash
    {
        Because of = () => name = _naming.GetColumnFamily(_versionWithDash);
        It should_construct_correctly_and_remove_dashes_from_name = () => name.ShouldEqual($"projasd_{_versionWithDash.Revision}_{_versionWithDash.Hash}");
    }

    class And_version_name_contains_upper_case
    {
        Because of = () => name = _naming.GetColumnFamily(_versionWithUpperCase);
        It should_construct_correctly_and_make_all_chars_from_name_lowercase = () => name.ShouldEqual($"projalabala_{_versionWithUpperCase.Revision}_{_versionWithUpperCase.Hash}");
    }

    class And_version_name_contains_upper_case_and_dashes
    {
        Because of = () => name = _naming.GetColumnFamily(_versionWithDashAndUpperCase);
        It should_construct_correctly = () => name.ShouldEqual($"projalabala_{_versionWithDashAndUpperCase.Revision}_{_versionWithDashAndUpperCase.Hash}");
    }
}

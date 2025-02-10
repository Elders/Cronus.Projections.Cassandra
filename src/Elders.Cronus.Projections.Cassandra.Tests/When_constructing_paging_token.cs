using Machine.Specifications;

namespace Elders.Cronus.Projections.Cassandra.Tests;

internal class When_constructing_paging_token
{
    private static long partitionId;
    private static byte[] cassandraToken;

    private static byte[] pagedToken;

    Establish context = () =>
    {
        partitionId = 2024123;
        cassandraToken = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    };

    class And_both_parameters_are_provided
    {
        Because of = () => pagedToken = ProjectionPaginationTokenFactory.Construct(partitionId, cassandraToken);
        It should_have_correct_legth = () => pagedToken.Length.ShouldEqual(18); // 10 + 8
    }

    class And_only_partitionId_is_provided
    {
        Because of = () => pagedToken = ProjectionPaginationTokenFactory.Construct(partitionId, null);
        It should_have_correct_legth = () => pagedToken.Length.ShouldEqual(8); // long is 8 bytes
    }
}

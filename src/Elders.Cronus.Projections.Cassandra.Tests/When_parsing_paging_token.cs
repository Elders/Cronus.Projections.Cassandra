using Machine.Specifications;
using System;

namespace Elders.Cronus.Projections.Cassandra.Tests;

internal class When_parsing_paging_token
{
    private static long _partitionId;
    private static byte[] _cassandraToken;

    private static byte[] _pagedTokenWithCassandraToken;
    private static byte[] _pagedTokenWithPartitionIdOnly;
    private static byte[] _notValidPagedToken;

    private static long _parsedPartitionId;
    private static byte[] _parsedCassandraToken;

    private static Exception _exception;

    Establish context = () =>
    {
        _partitionId = 2024123;
        _cassandraToken = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        _pagedTokenWithCassandraToken = ProjectionPaginationTokenFactory.Construct(_partitionId, _cassandraToken);
        _pagedTokenWithPartitionIdOnly = ProjectionPaginationTokenFactory.Construct(_partitionId, null);

        _notValidPagedToken = [1, 2, 3, 4, 5];
    };

    class And_paged_token_contains_partition_and_cassandra_token
    {
        Because of = () => (_parsedPartitionId, _parsedCassandraToken) = ProjectionPaginationTokenFactory.Parse(_pagedTokenWithCassandraToken);

        It should_have_the_same_pid = () => _partitionId.ShouldEqual(_parsedPartitionId);
        It should_have_the_same_cassandraToken = () => _cassandraToken.ShouldEqual(_parsedCassandraToken);
    }

    class And_only_partitionId_is_provided
    {
        Because of = () => (_parsedPartitionId, _parsedCassandraToken) = ProjectionPaginationTokenFactory.Parse(_pagedTokenWithPartitionIdOnly);

        It should_have_the_same_pid = () => _partitionId.ShouldEqual(_parsedPartitionId);
        It should_have_cassandraToken_to_be_null = () => _parsedCassandraToken.ShouldBeNull();
    }

    class And_token_is_too_short_and_not_valid
    {
        Because of = () => _exception = Catch.Exception(() => ProjectionPaginationTokenFactory.Parse(_notValidPagedToken));

        It should_fail = () => _exception.ShouldBeOfExactType<ArgumentException>();
    }
}

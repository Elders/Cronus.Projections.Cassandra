using System;

namespace Elders.Cronus.Projections.Cassandra
{
    public static class ProjectionPaginationTokenFactory
    {
        /// <summary>
        /// Constructs the partition id and the cassandra token as a byte array
        /// The first 8 bytes are reserved for the partition id the rest is the cassandra token
        /// The cassandra paging token CAN be null, in this case the byte[] will contain only the partitionId
        /// </summary>
        /// <param name="partitionId"></param>
        /// <param name="tokenFromCassandra"></param>
        /// <returns></returns>
        public static byte[] Construct(long partitionId, byte[] tokenFromCassandra)
        {
            byte[] partitionBytes = BitConverter.GetBytes(partitionId);
            byte[] result = new byte[8 + (tokenFromCassandra?.Length ?? 0)]; // account for when the cass token is null

            Array.Copy(partitionBytes, 0, result, 0, 8);

            if (tokenFromCassandra is not null)
                Array.Copy(tokenFromCassandra, 0, result, 8, tokenFromCassandra.Length);

            return result;
        }

        /// <summary>
        /// Parses the partition id and cassandra token from the paging token
        /// </summary>
        /// <param name="pagingToken">The paging token containing the partition id and token from cassandra</param>
        /// <param name="partitionId"></param>
        /// <param name="tokenFromCassandra"></param>
        /// <exception cref="ArgumentException"> When the token does not have a valid partition id</exception>
        public static (long PartitionId, byte[] CassandraToken) Parse(byte[] pagingToken)
        {
            if (pagingToken.Length < 8)
                throw new ArgumentException("The token is not valid");

            byte[] partitionBytes = new byte[8];
            Array.Copy(pagingToken, 0, partitionBytes, 0, 8);

            long partitionId = BitConverter.ToInt64(partitionBytes, 0);
            byte[] tokenFromCassandra = null;

            if (pagingToken.Length > 8)
            {
                tokenFromCassandra = new byte[pagingToken.Length - 8];
                Array.Copy(pagingToken, 8, tokenFromCassandra, 0, tokenFromCassandra.Length);
            }

            return (partitionId, tokenFromCassandra);
        }
    }
}

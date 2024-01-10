using Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    internal sealed class PagingInfo
    {
        public PagingInfo()
        {
            HasMore = true;
        }

        public byte[] Token { get; set; }

        public bool HasMore { get; set; }

        public bool HasToken() => Token is null == false;

        public static PagingInfo From(RowSet result)
        {
            return new PagingInfo()
            {
                Token = result.PagingState,
                HasMore = result.PagingState is null == false
            };
        }
    }
}

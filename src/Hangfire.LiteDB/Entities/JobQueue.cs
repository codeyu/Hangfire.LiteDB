using System;
using LiteDB;

namespace Hangfire.LiteDB.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class JobQueue
    {
        private DateTime? _fetchedAt;

        /// <summary>
        /// 
        /// </summary>
        [BsonId]
        public ObjectId Id { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int JobId { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Queue { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime? FetchedAt { get { return _fetchedAt.HasValue ? _fetchedAt.Value.ToUniversalTime() : (DateTime?)null; } set { _fetchedAt = value; } }
    }
}
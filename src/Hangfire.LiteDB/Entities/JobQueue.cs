using System;
using LiteDB;

namespace Hangfire.LiteDB.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class JobQueue
    {
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
        public DateTime? FetchedAt { get; set; }
    }
}
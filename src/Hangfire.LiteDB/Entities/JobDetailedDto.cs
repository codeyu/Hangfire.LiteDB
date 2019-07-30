using System;
using System.Collections.Generic;
using LiteDB;

namespace Hangfire.LiteDB.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class JobDetailedDto
    {
        private DateTime _createdAt;
        private DateTime? _expireAt;
        private DateTime? _fetchedAt;

        /// <summary>
        /// 
        /// </summary>
        public int Id { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string InvocationData { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string Arguments { get; set; }
        /// <summary>
        /// 
        /// </summary>
        /// <summary>
        /// 
        /// </summary>
        public DateTime? ExpireAt { get { return _expireAt.HasValue ? _expireAt.Value.ToUniversalTime() : default; } set { _expireAt = value; } }
        /// <summary>
        /// 
        /// </summary>
        public DateTime? FetchedAt { get { return _fetchedAt.HasValue ? _fetchedAt.Value.ToUniversalTime() : default; } set { _fetchedAt = value; } }
        /// <summary>
        /// 
        /// </summary>
        public ObjectId StateId { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string StateName { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string StateReason { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, string> StateData { get; set; }
    }
}
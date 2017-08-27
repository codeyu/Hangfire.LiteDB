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
        public DateTime CreatedAt { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public DateTime? ExpireAt { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public DateTime? FetchedAt { get; set; }
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
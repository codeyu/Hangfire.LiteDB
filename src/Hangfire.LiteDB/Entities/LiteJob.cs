using System;
using System.Collections.Generic;
using LiteDB;

namespace Hangfire.LiteDB.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class LiteJob
    {
        private DateTime _createdAt;
        private DateTime? _expireAt;

        /// <summary>
        /// 
        /// </summary>
        public int Id { get; set; }
        
        /// <summary>
        /// 
        /// </summary>
        public string IdString => Id.ToString();

        /// <summary>
        /// 
        /// </summary>
        public string StateName { get; set; }

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
        public Dictionary<string, string> Parameters { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<LiteState> StateHistory { get; set; } = new List<LiteState>();

        /// <summary>
        /// 
        /// </summary>
        public DateTime CreatedAt { get { return _createdAt.ToUniversalTime(); } set { _createdAt = value; } }

        /// <summary>
        /// 
        /// </summary>
        public DateTime? ExpireAt { get { return _expireAt.HasValue ? _expireAt.Value.ToUniversalTime() : default; } set { _expireAt = value; } }

    }
}
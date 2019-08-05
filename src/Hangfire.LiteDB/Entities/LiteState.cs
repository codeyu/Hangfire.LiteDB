using System;
using System.Collections.Generic;

namespace Hangfire.LiteDB.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class LiteState
    {
        private DateTime _createdAt;

        /// <summary>
        /// 
        /// </summary>
        public int JobId { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string Reason { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public DateTime CreatedAt { get { return _createdAt.ToUniversalTime(); } set { _createdAt = value; } }
        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, string> Data { get; set; }
    }
}
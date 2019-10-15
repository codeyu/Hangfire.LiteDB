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
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime? ExpireAt { get; set; }

    }
}
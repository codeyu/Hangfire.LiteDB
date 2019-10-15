using System;

namespace Hangfire.LiteDB.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class Server
    {
        /// <summary>
        /// 
        /// </summary>
        public string Id { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public DateTime LastHeartbeat { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string Data { get; set; }
    }
}
using System;

namespace Hangfire.LiteDB.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class Server
    {
        private DateTime _lastHeartbeat;

        /// <summary>
        /// 
        /// </summary>
        public string Id { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public DateTime LastHeartbeat { get { return _lastHeartbeat.ToUniversalTime(); } set { _lastHeartbeat = value; } }
        /// <summary>
        /// 
        /// </summary>
        public string Data { get; set; }
    }
}
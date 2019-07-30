﻿using System;
using System.Collections.Generic;

namespace Hangfire.LiteDB.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class ServerData
    {
        private DateTime? _startedAt;

        /// <summary>
        /// 
        /// </summary>
        public int WorkerCount { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public IEnumerable<string> Queues { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public DateTime? StartedAt { get { return _startedAt.HasValue ? _startedAt.Value.ToUniversalTime() : default; } set { _startedAt = value; } }
    }
}
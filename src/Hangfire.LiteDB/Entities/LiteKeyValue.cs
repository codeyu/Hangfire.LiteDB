using System;
using LiteDB;

namespace Hangfire.LiteDB.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class LiteKeyValue
    {

        /// <summary>
        /// 
        /// </summary>
        [BsonId]
        public ObjectId Id { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public object Value { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class LiteExpiringKeyValue : LiteKeyValue
    {
        /// <summary>
        /// 
        /// </summary>
        public DateTime? ExpireAt { get; set; }
    }
}
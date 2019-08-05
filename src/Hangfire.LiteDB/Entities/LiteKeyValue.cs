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
        private DateTime? _expireAt;

        /// <summary>
        /// 
        /// </summary>
        public DateTime? ExpireAt { get { return _expireAt.HasValue ? _expireAt.Value.ToUniversalTime() : (DateTime?)null; } set { _expireAt = value; } }
    }
}
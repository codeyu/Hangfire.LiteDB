using LiteDB;

namespace Hangfire.LiteDB.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class LiteSchema
    {
        /// <summary>
        /// 
        /// </summary>
        [BsonId]
        public int Version { get; set; }
    }
}
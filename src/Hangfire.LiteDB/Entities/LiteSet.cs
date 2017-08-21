using System.Collections.Generic;

namespace Hangfire.LiteDB.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class LiteSet : LiteExpiringKeyValue
    {
        
        /// <summary>
        /// 
        /// </summary>
        public double Score { get; set; }
    }
}
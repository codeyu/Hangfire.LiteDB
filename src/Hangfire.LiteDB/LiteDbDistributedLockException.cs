using System;

namespace Hangfire.LiteDB
{
    /// <summary>
    /// 
    /// </summary>
    public class LiteDbDistributedLockException : Exception
    {
        /// <summary>
        /// Creates exception with inner exception
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        public LiteDbDistributedLockException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
using System;

namespace Hangfire.LiteDB
{
    /// <summary>
    /// 
    /// </summary>
    public static class ObjectExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="name"></param>
        public static void ThrowIfNull(this object value, string name)
        {
            if (value == null) {
                throw new ArgumentNullException(name);
            }
        }
    }
}
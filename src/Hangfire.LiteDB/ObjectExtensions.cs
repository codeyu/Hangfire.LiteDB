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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public static long ToInt64(this object value) 
        {
            long longValue = 0L;

            try
            {
                longValue = Convert.ToInt64(value);
            }
            catch (Exception)
            {
                //Nothing..
            }

            return longValue;
        }
    }
}
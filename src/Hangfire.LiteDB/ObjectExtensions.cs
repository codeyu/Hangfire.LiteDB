using System;

namespace Hangfire.LiteDB
{
    public static class ObjectExtensions
    {
        public static void ThrowIfNull(this object value, string name)
        {
            if (value == null) {
                throw new ArgumentNullException(name);
            }
        }
    }
}
using System;
using System.IO;

namespace Hangfire.LiteDB.Test.Utils
{
#pragma warning disable 1591
    public static class ConnectionUtils
    {
        private const string Ext = "db";
        
        private static string GetConnectionString()
        {
            return Path.GetFullPath(string.Format("Hangfire-LiteDB-Tests.{0}", Ext));
        }

        public static LiteDbStorage CreateStorage()
        {
            var storageOptions = new LiteDbStorageOptions();
            
            return CreateStorage(storageOptions);
        }

        public static LiteDbStorage CreateStorage(LiteDbStorageOptions storageOptions)
        {
            return new LiteDbStorage(GetConnectionString(), storageOptions);
        }

        public static HangfireDbContext CreateConnection()
        {
            return CreateStorage().Connection;
        }
    }
#pragma warning restore 1591
}
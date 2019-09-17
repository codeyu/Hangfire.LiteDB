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
            var pathDb = Path.GetFullPath(string.Format("Hangfire-LiteDB-Tests.{0}", Ext));
            return @"Filename=" + pathDb + "; mode=Exclusive";
        }

        public static LiteDbStorage CreateStorage()
        {
            var storageOptions = new LiteDbStorageOptions();
            
            return CreateStorage(storageOptions);
        }

        public static LiteDbStorage CreateStorage(LiteDbStorageOptions storageOptions)
        {
            var connectionString = GetConnectionString();
            return new LiteDbStorage(connectionString, storageOptions);
        }

        public static HangfireDbContext CreateConnection()
        {
            return CreateStorage().Connection;
        }
    }
#pragma warning restore 1591
}
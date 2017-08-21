using System;

namespace Hangfire.LiteDB.Test.Utils
{
#pragma warning disable 1591
    public static class ConnectionUtils
    {
        private const string DatabaseVariable = "Hangfire_Mongo_DatabaseName";
        private const string ConnectionStringTemplateVariable = "Hangfire_Mongo_ConnectionStringTemplate";

        private const string DefaultDatabaseName = @"Hangfire-Mongo-Tests";
        private const string DefaultConnectionStringTemplate = @"mongodb://localhost";

        private static string GetDatabaseName()
        {
            return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;
        }

        private static string GetConnectionString()
        {
            return string.Format(GetConnectionStringTemplate(), GetDatabaseName());
        }

        private static string GetConnectionStringTemplate()
        {
            return Environment.GetEnvironmentVariable(ConnectionStringTemplateVariable) ?? DefaultConnectionStringTemplate;
        }

        public static LiteDbStorage CreateStorage()
        {
            var storageOptions = new LiteDbStorageOptions();
            
            return CreateStorage(storageOptions);
        }

        public static LiteDbStorage CreateStorage(LiteDbStorageOptions storageOptions)
        {
            return new LiteDbStorage(GetConnectionString(), GetDatabaseName(), storageOptions);
        }

        public static HangfireDbContext CreateConnection()
        {
            return CreateStorage().Connection;
        }
    }
#pragma warning restore 1591
}
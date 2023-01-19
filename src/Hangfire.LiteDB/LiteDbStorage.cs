using System;
using System.Collections.Generic;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using LiteDB;

namespace Hangfire.LiteDB
{
    /// <summary>
    /// 
    /// </summary>
    public class LiteDbStorage : JobStorage
    {
        private readonly LiteDbStorageOptions _storageOptions;

        /// <summary>
        /// Constructs Job Storage by database connection string
        /// </summary>
        /// <param name="connectionString">LiteDB connection string</param>
        public LiteDbStorage(string connectionString) : this(connectionString,  new LiteDbStorageOptions())
        {
        }

        /// <summary>
        /// Constructs Job Storage by database connection string
        /// </summary>
        /// <param name="liteDatabase">LiteDB connection string</param>
        public LiteDbStorage(LiteRepository liteDatabase) : this(HangfireDbContext.Instance(liteDatabase), new LiteDbStorageOptions())
        {
            if (liteDatabase == null)
            {
                throw new ArgumentNullException(nameof(liteDatabase));
            }
        }

        /// <summary>
        /// Constructs Job Storage by database connection string and options
        /// </summary>
        /// <param name="connectionString">LiteDB connection string</param>
        /// <param name="storageOptions">Storage options</param>
        public LiteDbStorage(string connectionString, LiteDbStorageOptions storageOptions) : this(HangfireDbContext.Instance(connectionString, storageOptions.Prefix),storageOptions)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }
        }
            

        /// <summary>
        /// Constructs Job Storage by database connection string and options
        /// </summary>
        /// <param name="connection">LiteDB connection string</param>
        /// <param name="storageOptions">Storage options</param>
        private LiteDbStorage(HangfireDbContext connection, LiteDbStorageOptions storageOptions)
        {
            //_connectionString = connectionString;
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));

            Connection = connection;
            Connection.Init(_storageOptions);
            var defaultQueueProvider = new LiteDbJobQueueProvider(_storageOptions);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }

        /// <summary>
        /// Database context
        /// </summary>
        public HangfireDbContext Connection { get; }

        /// <summary>
        /// Queue providers collection
        /// </summary>
        public PersistentJobQueueProviderCollection QueueProviders { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IMonitoringApi GetMonitoringApi()
        {
            
            return new LiteDbMonitoringApi(Connection, QueueProviders);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IStorageConnection GetConnection()
        {
            return new LiteDbConnection(Connection, _storageOptions, QueueProviders);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="logger"></param>
        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for LiteDB job storage:");
        }

        /// <summary>
        /// Opens connection to database
        /// </summary>
        /// <returns>Database context</returns>
        public HangfireDbContext CreateAndOpenConnection()
        {
            return Connection;
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            return $"prefix: {_storageOptions.Prefix}";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, _storageOptions.JobExpirationCheckInterval);
            yield return new CountersAggregator(this, _storageOptions.CountersAggregateInterval);
        }
    }
}

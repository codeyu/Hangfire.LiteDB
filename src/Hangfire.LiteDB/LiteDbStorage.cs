using System;
using System.Collections.Generic;
using Hangfire.LiteDB.StateHandlers;
using Hangfire.Logging;
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
        private readonly string _connectionString;

        /// <summary>
        /// 
        /// </summary>
        private ConnectionString LiteConnectionString { get; }

        private readonly LiteDbStorageOptions _storageOptions;

        /// <summary>
        /// Constructs Job Storage by database connection string
        /// </summary>
        /// <param name="connectionString">MongoDB connection string</param>
        public LiteDbStorage(string connectionString)
            : this(connectionString,  new LiteDbStorageOptions())
        {
        }

        /// <summary>
        /// Constructs Job Storage by database connection string and options
        /// </summary>
        /// <param name="connectionString">LiteDB connection string</param>
        /// <param name="storageOptions">Storage options</param>
        /// <param name="mapper"></param>
        public LiteDbStorage(string connectionString, LiteDbStorageOptions storageOptions, BsonMapper mapper = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }
           

            _connectionString = connectionString;
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));

            Connection = new HangfireDbContext(connectionString, mapper, storageOptions.Prefix);
            Connection.Init(_storageOptions);
            var defaultQueueProvider = new LiteDbJobQueueProvider(_storageOptions);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="liteConnStr"></param>
        public LiteDbStorage(ConnectionString liteConnStr)
            : this(liteConnStr, new LiteDbStorageOptions())
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="liteConnStr"></param>
        /// <param name="storageOptions">Storage options</param>
        /// <param name="mapper"></param>
        public LiteDbStorage(ConnectionString liteConnStr, LiteDbStorageOptions storageOptions, BsonMapper mapper = null)
        {
         
            LiteConnectionString = liteConnStr ?? throw new ArgumentNullException(nameof(liteConnStr));
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));

            Connection = new HangfireDbContext(liteConnStr, mapper, _storageOptions.Prefix);
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
            return _connectionString != null ? new HangfireDbContext(_connectionString, null, _storageOptions.Prefix) : new HangfireDbContext(LiteConnectionString, null, _storageOptions.Prefix);
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            
            return $"Connection string: {_connectionString},  prefix: {_storageOptions.Prefix}";
        }

        /// <summary>
        /// Returns collection of state handers
        /// </summary>
        /// <returns>Collection of state handers</returns>
        public override IEnumerable<IStateHandler> GetStateHandlers()
        {
            yield return new FailedStateHandler();
            yield return new ProcessingStateHandler();
            yield return new SucceededStateHandler();
            yield return new DeletedStateHandler();
        }
    }
}
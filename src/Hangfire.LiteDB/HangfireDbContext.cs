using System;
using System.IO;
using Hangfire.LiteDB.Entities;
using LiteDB;
namespace Hangfire.LiteDB
{
    /// <summary>
    /// Represents Mongo database context for Hangfire
    /// </summary>
    public sealed class HangfireDbContext : IDisposable
    {
        private readonly string _prefix;

        internal LiteDatabase Database { get; }

        /// <summary>
        /// Starts LiteDB database using a connection string for file system database
        /// </summary>
        /// <param name="connectionString">Connection string for LiteDB database</param>
        /// <param name="prefix">Collections prefix</param>
        /// <param name="mapper"></param>
        public HangfireDbContext(string connectionString, BsonMapper mapper = null, string prefix = "hangfire")
        {
            _prefix = prefix;

            var client = new LiteRepository(connectionString, mapper);

            Database = client.Database;

            ConnectionId = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Starts LiteDB database using a connection string for file system database
        /// </summary>
        /// <param name="mapper"></param>
        /// <param name="prefix">Collections prefix</param>
        /// <param name="connectionString"></param>
        public HangfireDbContext(ConnectionString connectionString, BsonMapper mapper = null, string prefix = "hangfire")
        {
            _prefix = prefix;

            var client = new LiteRepository(connectionString, mapper);

            Database = client.Database;

            ConnectionId = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Starts LiteDB database using a Stream disk
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="mapper"></param>
        /// <param name="password"></param>
        public HangfireDbContext(Stream stream, BsonMapper mapper = null, string password = null)
        {
            var client = new LiteRepository(stream, mapper, password);
            Database = client.Database;
            ConnectionId = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Mongo database connection identifier
        /// </summary>
        public string ConnectionId { get; private set; }

        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public LiteCollection<LiteKeyValue> StateDataKeyValue =>
            Database.GetCollection<LiteKeyValue>(_prefix + $".{nameof(LiteKeyValue)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public LiteCollection<LiteExpiringKeyValue> StateDataExpiringKeyValue =>
            Database.GetCollection<LiteExpiringKeyValue>(_prefix + $".{nameof(StateDataExpiringKeyValue)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public LiteCollection<LiteHash> StateDataHash =>
            Database.GetCollection<LiteHash>(_prefix + $".{nameof(LiteHash)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public LiteCollection<LiteList> StateDataList =>
            Database.GetCollection<LiteList>(_prefix + $".{nameof(LiteList)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public LiteCollection<LiteSet> StateDataSet =>
            Database.GetCollection<LiteSet>(_prefix + $".{nameof(LiteSet)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public LiteCollection<Counter> StateDataCounter =>
            Database.GetCollection<Counter>(_prefix + $".{nameof(Counter)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public LiteCollection<AggregatedCounter> StateDataAggregatedCounter =>
            Database.GetCollection<AggregatedCounter>(_prefix + $".{nameof(AggregatedCounter)}");

        /// <summary>
        /// Reference to collection which contains distributed locks
        /// </summary>
        public LiteCollection<DistributedLock> DistributedLock => Database
            .GetCollection<DistributedLock>(_prefix + ".locks");

        /// <summary>
        /// Reference to collection which contains jobs
        /// </summary>
        public LiteCollection<LiteJob> Job => Database.GetCollection<LiteJob>(_prefix + ".job");

        /// <summary>
        /// Reference to collection which contains jobs queues
        /// </summary>
        public LiteCollection<JobQueue> JobQueue =>
            Database.GetCollection<JobQueue>(_prefix + ".jobQueue");

        /// <summary>
        /// Reference to collection which contains schemas
        /// </summary>
        public LiteCollection<LiteSchema> Schema => Database.GetCollection<LiteSchema>(_prefix + ".schema");

        /// <summary>
        /// Reference to collection which contains servers information
        /// </summary>
        public LiteCollection<Entities.Server> Server => Database.GetCollection<Entities.Server>(_prefix + ".server");

        /// <summary>
        /// Initializes intial collections schema for Hangfire
        /// </summary>
        public void Init(LiteDbStorageOptions storageOptions)
        {
            //var migrationManager = new LiteDbStorageOptions(storageOptions);
            //migrationManager.Migrate(this);
        }


        /// <summary>
        /// Disposes the object
        /// </summary>
        public void Dispose()
        {
        }
    }
}
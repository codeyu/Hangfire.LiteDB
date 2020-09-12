using System;
using System.Diagnostics;
using Hangfire.LiteDB.Entities;
using LiteDB;
using Newtonsoft.Json;

namespace Hangfire.LiteDB
{
    /// <summary>
    /// Represents LiteDB database context for Hangfire
    /// </summary>
    public sealed class HangfireDbContext
    {
        private readonly string _prefix;

        /// <summary>
        /// 
        /// </summary>
        public ILiteDatabase Database { get; }

        /// <summary>
        /// 
        /// </summary>
        public LiteRepository Repository { get; }

        /// <summary>
        /// 
        /// </summary>
        public LiteDbStorageOptions StorageOptions { get; private set; }

        private static readonly object Locker = new object();
        private static volatile HangfireDbContext _instance;

        /// <summary>
        /// Starts LiteDB database using a connection string for file system database
        /// </summary>
        /// <param name="connectionString">Connection string for LiteDB database</param>
        /// <param name="prefix">Collections prefix</param>
        private HangfireDbContext(string connectionString, string prefix = "hangfire")
        {
            _prefix = prefix;

            //UTC - LiteDB
            BsonMapper.Global.ResolveMember += (type, memberInfo, member) =>
            {
                if (member.DataType == typeof(DateTime?) || member.DataType == typeof(DateTime))
                {
                    member.Deserialize = (v, m) => v != null ? v.AsDateTime.ToUniversalTime() : (DateTime?)null;
                    member.Serialize = (o, m) => new BsonValue(((DateTime?)o).HasValue ? ((DateTime?)o).Value.ToUniversalTime() : (DateTime?)null);
                }
            };

            //UTC - Internal JSON
            GlobalConfiguration.Configuration
                .UseSerializerSettings(new JsonSerializerSettings() {
                    DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                    DateFormatHandling = DateFormatHandling.IsoDateFormat,
                    DateFormatString = "yyyy-MM-dd HH:mm:ss.fff"
                });

            Repository = new LiteRepository(connectionString);
            
            Database = Repository.Database;

            ConnectionId = Guid.NewGuid().ToString();

            //Create Indexes
            StateDataKeyValue.EnsureIndex("Key");
            StateDataExpiringKeyValue.EnsureIndex("Key");
            StateDataHash.EnsureIndex("Key");
            StateDataList.EnsureIndex("Key");
            StateDataSet.EnsureIndex("Key");
            StateDataCounter.EnsureIndex("Key");
            StateDataAggregatedCounter.EnsureIndex("Key");
            DistributedLock.EnsureIndex("Resource", true);
            Job.EnsureIndex("Id");
            Job.EnsureIndex("StateName");
            Job.EnsureIndex("CreatedAt");
            Job.EnsureIndex("ExpireAt");
            Job.EnsureIndex("FetchedAt");
            JobQueue.EnsureIndex("JobId");
            JobQueue.EnsureIndex("Queue");
            JobQueue.EnsureIndex("FetchedAt");
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public static HangfireDbContext Instance(string connectionString, string prefix = "hangfire")
        {
            if (_instance != null) return _instance;
            lock (Locker)
            {
                if (_instance == null)
                {
                    _instance = new HangfireDbContext(connectionString, prefix);
                }
            }

            return _instance;
        }

        /// <summary>
        /// LiteDB database connection identifier
        /// </summary>
        public string ConnectionId { get; }

        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollection<LiteKeyValue> StateDataKeyValue =>
            Database.GetCollection<LiteKeyValue>(_prefix + $"_{nameof(LiteKeyValue)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollection<LiteExpiringKeyValue> StateDataExpiringKeyValue =>
            Database.GetCollection<LiteExpiringKeyValue>(_prefix + $"_{nameof(StateDataExpiringKeyValue)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollection<LiteHash> StateDataHash =>
            Database.GetCollection<LiteHash>(_prefix + $"_{nameof(LiteHash)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollection<LiteList> StateDataList =>
            Database.GetCollection<LiteList>(_prefix + $"_{nameof(LiteList)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollection<LiteSet> StateDataSet =>
            Database.GetCollection<LiteSet>(_prefix + $"_{nameof(LiteSet)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollection<Counter> StateDataCounter =>
            Database.GetCollection<Counter>(_prefix + $"_{nameof(Counter)}");
        /// <summary>
        /// Reference to collection which contains various state information
        /// </summary>
        public ILiteCollection<AggregatedCounter> StateDataAggregatedCounter =>
            Database.GetCollection<AggregatedCounter>(_prefix + $"_{nameof(AggregatedCounter)}");

        /// <summary>
        /// Reference to collection which contains distributed locks
        /// </summary>
        public ILiteCollection<DistributedLock> DistributedLock => Database
            .GetCollection<DistributedLock>(_prefix + "_locks");

        /// <summary>
        /// Reference to collection which contains jobs
        /// </summary>
        public ILiteCollection<LiteJob> Job => Database.GetCollection<LiteJob>(_prefix + "_job");

        /// <summary>
        /// Reference to collection which contains jobs queues
        /// </summary>
        public ILiteCollection<JobQueue> JobQueue =>
            Database.GetCollection<JobQueue>(_prefix + "_jobQueue");

        /// <summary>
        /// Reference to collection which contains schemas
        /// </summary>
        public ILiteCollection<LiteSchema> Schema => Database.GetCollection<LiteSchema>(_prefix + "_schema");

        /// <summary>
        /// Reference to collection which contains servers information
        /// </summary>
        public ILiteCollection<Entities.Server> Server => Database.GetCollection<Entities.Server>(_prefix + "_server");

        /// <summary>
        /// Initializes intial collections schema for Hangfire
        /// </summary>
        public void Init(LiteDbStorageOptions storageOptions)
        {
            StorageOptions = storageOptions;
        }
    }
}
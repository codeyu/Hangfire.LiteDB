using System;
using System.Linq.Expressions;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using LiteDB;

namespace Hangfire.LiteDB
{
    /// <summary>
    /// Represents Hangfire expiration manager for LiteDB database
    /// </summary>
    public class ExpirationManager : IBackgroundProcess, IServerComponent
    {
        private const string DistributedLockKey = "locks:expirationmanager";
        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromMinutes(5);

        // This value should be high enough to optimize the deletion as much, as possible,
        // reducing the number of queries. But low enough to cause lock escalations (it
        // appears, when ~5000 locks were taken, but this number is a subject of version).
        // Note, that lock escalation may also happen during the cascade deletions for
        // State (3-5 rows/job usually) and JobParameters (2-3 rows/job usually) tables.
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly ILog Logger = LogProvider.For<ExpirationManager>();

        private readonly LiteDbStorage _storage;
        private readonly TimeSpan _checkInterval;
        private static readonly string[] ProcessedTables =
        {
            "Counter",
            "AggregatedCounter",
            "LiteJob",
            "LiteList",
            "LiteSet",
            "LiteHash"
        };
        /// <summary>
        /// Constructs expiration manager with one hour checking interval
        /// </summary>
        /// <param name="storage">LiteDb storage</param>
        public ExpirationManager(LiteDbStorage storage)
            : this(storage, TimeSpan.FromHours(1))
        {
        }

        /// <summary>
        /// Constructs expiration manager with specified checking interval
        /// </summary>
        /// <param name="storage">LiteDB storage</param>
        /// <param name="checkInterval">Checking interval</param>
        public ExpirationManager(LiteDbStorage storage, TimeSpan checkInterval)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _checkInterval = checkInterval;
        }

        /// <summary>
        /// Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="context">Background processing context</param>
        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        /// <summary>
        /// Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public void Execute(CancellationToken cancellationToken)
        {
            HangfireDbContext connection = _storage.CreateAndOpenConnection();
            DateTime now = DateTime.UtcNow;

            RemoveExpiredRecord(connection, connection.Job, _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);
            RemoveExpiredRecord(connection, connection.StateDataAggregatedCounter, _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);
            RemoveExpiredRecord(connection, connection.StateDataCounter, _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);
            RemoveExpiredRecord(connection, connection.StateDataHash, _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);
            RemoveExpiredRecord(connection, connection.StateDataSet, _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);
            RemoveExpiredRecord(connection, connection.StateDataList, _ => _.ExpireAt != null && _.ExpireAt.Value.ToUniversalTime() < now);

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            return "LiteDB Expiration Manager";
        }

        private void RemoveExpiredRecord<TEntity>(HangfireDbContext db, ILiteCollection<TEntity> collection, Expression<Func<TEntity, bool>> expression)
        {
            Logger.DebugFormat("Removing outdated records from table '{0}'...", collection.Name);
            int result = 0;

            try
            {
                var _lock = new LiteDbDistributedLock(DistributedLockKey, DefaultLockTimeout,
                    db, db.StorageOptions);

                using (_lock)
                {
                    result = collection.DeleteMany(expression);
                }
            }
            catch (DistributedLockTimeoutException e) when (e.Resource == DistributedLockKey)
            {
                // DistributedLockTimeoutException here doesn't mean that outdated records weren't removed.
                // It just means another Hangfire server did this work.
                Logger.Log(
                    LogLevel.Debug,
                    () => $@"An exception was thrown during acquiring distributed lock on the {DistributedLockKey} resource within {DefaultLockTimeout.TotalSeconds} seconds. Outdated records were not removed. It will be retried in {_checkInterval.TotalSeconds} seconds.",
                    e);
            }
            catch (Exception e)
            {
                Logger.Log(LogLevel.Error, () => $"Error in RemoveExpireRows Method. Details: {e.ToString()}", e);
            }

#if DEBUG
            Logger.DebugFormat(result.ToString());
#endif
        }
    }
}
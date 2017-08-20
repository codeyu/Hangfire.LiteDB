using System;

namespace Hangfire.LiteDB
{
    /// <summary>
    /// 
    /// </summary>
    public class LiteDbJobQueueProvider
        : IPersistentJobQueueProvider
    {
        private readonly LiteDbStorageOptions _storageOptions;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="storageOptions"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public LiteDbJobQueueProvider(LiteDbStorageOptions storageOptions)
        {
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public IPersistentJobQueue GetJobQueue(HangfireDbContext connection)
        {
            return new LiteDbJobQueue(connection, _storageOptions);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi(HangfireDbContext connection)
        {
            return new LiteDbJobQueueMonitoringApi(connection);
        }
    }
}
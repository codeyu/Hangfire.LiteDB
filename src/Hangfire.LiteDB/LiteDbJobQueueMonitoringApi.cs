using System;
using System.Collections.Generic;
using System.Linq;
using LiteDB;
namespace Hangfire.LiteDB
{
    /// <summary>
    /// 
    /// </summary>
    public class LiteDbJobQueueMonitoringApi
    : IPersistentJobQueueMonitoringApi
    {
        private readonly HangfireDbContext _connection;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        public LiteDbJobQueueMonitoringApi(HangfireDbContext connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetQueues()
        {
            return _connection.JobQueue
                .FindAll()
                .Select(_ => _.Queue)
                .ToList().Distinct().ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="from"></param>
        /// <param name="perPage"></param>
        /// <returns></returns>
        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            var jobIds = _connection.JobQueue
                .Find(_ => _.Queue== queue && _.FetchedAt==null)
                .Skip(from)
                .Take(perPage)
                .Select(_ => _.JobId)
                .ToList();
            var jobs = _connection.Job.Find(Query.In("IdString", jobIds.ToBsonValueEnumerable())).Where(x=>x.StateHistory.Length > 0);
            return jobs.Select(_=>_.IdString);
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="from"></param>
        /// <param name="perPage"></param>
        /// <returns></returns>
        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            return _connection.JobQueue
                .Find(_ => _.Queue== queue && _.FetchedAt!=null)
                .Skip(from)
                .Take(perPage)
                .Select(_ => _.JobId)
                .ToList()
                .Where(jobQueueJobId =>
                {
                    var job = _connection.Job.Find(_ => _.IdString==jobQueueJobId).FirstOrDefault();
                    return job != null;
                }).ToArray();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            var enqueuedCount = _connection.JobQueue.Count(_ => _.Queue== queue && _.FetchedAt==null);

            var fetchedCount = _connection.JobQueue.Count(_ => _.Queue== queue && _.FetchedAt!=null);

            return new EnqueuedAndFetchedCountDto
            {
                EnqueuedCount = enqueuedCount,
                FetchedCount = fetchedCount
            };
        }
    }
}
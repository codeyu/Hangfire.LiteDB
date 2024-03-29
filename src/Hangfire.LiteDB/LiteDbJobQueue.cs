using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.LiteDB.Entities;
using Hangfire.Storage;
using LiteDB;

namespace Hangfire.LiteDB
{
    /// <summary>
    /// 
    /// </summary>
    public class LiteDbJobQueue : IPersistentJobQueue
    {
        private readonly LiteDbStorageOptions _storageOptions;

        private readonly HangfireDbContext _connection;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="storageOptions"></param>
        public LiteDbJobQueue(HangfireDbContext connection, LiteDbStorageOptions storageOptions)
        {
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="queues"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null)
            {
                throw new ArgumentNullException(nameof(queues));
            }

            if (queues.Length == 0)
            {
                throw new ArgumentException("Queue array must be non-empty.", nameof(queues));
            }


            JobQueue fetchedJob = null;
            while (fetchedJob == null)
            {
                cancellationToken.ThrowIfCancellationRequested();


                foreach (var queue in queues)
                {
                    var lockQueue = string.Intern($"f13333e1-a0c8-48c8-bf8c-788e89030329_{queue}");
                    lock (lockQueue)
                    {
                        fetchedJob =
                            _connection.JobQueue.FindOne(x => x.FetchedAt == null && x.Queue == queue);

                        if (fetchedJob != null)
                        {
                            fetchedJob.FetchedAt = DateTime.UtcNow;
                            _connection.JobQueue.Update(fetchedJob);
                            break;
                        }
                    }
                }

                if (fetchedJob == null)
                    foreach (var queue in queues)
                    {
                        var lockQueue = string.Intern($"f13333e1-a0c8-48c8-bf8c-788e89030329_{queue}");
                        lock (lockQueue)
                        {
                            fetchedJob =
                                _connection.JobQueue.FindOne(x =>
                                    x.FetchedAt <
                                    DateTime.UtcNow.AddSeconds(
                                        _storageOptions.InvisibilityTimeout.Negate().TotalSeconds) && x.Queue == queue);

                            if (fetchedJob != null)
                            {
                                fetchedJob.FetchedAt = DateTime.UtcNow;
                                _connection.JobQueue.Update(fetchedJob);
                                break;
                            }
                        }
                    }

                if (fetchedJob == null)
                {
                    // ...and we are out of fetch conditions as well.
                    // Wait for a while before polling again.
                    cancellationToken.WaitHandle.WaitOne(_storageOptions.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }

            return new LiteDbFetchedJob(_connection, fetchedJob.Id, fetchedJob.JobId, fetchedJob.Queue);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="jobId"></param>
        public void Enqueue(string queue, string jobId)
        {
            _connection.JobQueue.Insert(new JobQueue
            {
                JobId = int.Parse(jobId),
                Queue = queue
            });
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.LiteDB.Entities;
using Hangfire.LiteDB.Test.Utils;
using Xunit;

namespace Hangfire.LiteDB.Test.PersistentJobQueue.LiteDB
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDbJobQueueMonitoringApiFacts
    {
        private const string QueueName1 = "queueName1";
        private const string QueueName2 = "queueName2";

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new LiteDbJobQueueMonitoringApi(null));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnEmpty_WhenNoQueuesExist()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                var queues = liteDbJobQueueMonitoringApi.GetQueues();

                Assert.Empty(queues);
            });
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnOneQueue_WhenOneQueueExists()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                CreateJobQueueDto(connection, QueueName1, false);

                var queues = liteDbJobQueueMonitoringApi.GetQueues().ToList();

                Assert.Equal(1, queues.Count);
                Assert.Equal(QueueName1, queues.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnTwoUniqueQueues_WhenThreeNonUniqueQueuesExist()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName2, false);

                var queues = liteDbJobQueueMonitoringApi.GetQueues().ToList();

                Assert.Equal(2, queues.Count);
                Assert.True(queues.Contains(QueueName1));
                Assert.True(queues.Contains(QueueName2));
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10);

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnEmpty_WhenOneJobWithAFetchedStateExists()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(1, enqueuedJobIds.Count);
                Assert.Equal(jobQueueDto.JobId, enqueuedJobIds.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto3 = CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto3.JobId));
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName2, false);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10);

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnEmpty_WhenOneJobWithNonFetchedStateExists()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(1, enqueuedJobIds.Count);
                Assert.Equal(jobQueueDto.JobId, enqueuedJobIds.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto3 = CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto3.JobId));
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, true);
                CreateJobQueueDto(connection, QueueName2, true);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            UseConnection(connection =>
            {
                var liteDbJobQueueMonitoringApi = CreateLiteDbJobQueueMonitoringApi(connection);

                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, true);
                CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = liteDbJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
            });
        }

        private static JobQueue CreateJobQueueDto(HangfireDbContext connection, string queue, bool isFetched)
        {
            var job = new LiteJob
            {
                CreatedAt = DateTime.Now,
                StateHistory = new List<LiteState>()
            };

            connection.Job.Insert(job);

            var jobQueue = new JobQueue
            {
                Queue = queue,
                JobId = job.Id
            };

            if (isFetched)
            {
                jobQueue.FetchedAt = DateTime.Now.AddDays(-1);
            }

            connection.JobQueue.Insert(jobQueue);

            return jobQueue;
        }

        private static LiteDbJobQueueMonitoringApi CreateLiteDbJobQueueMonitoringApi(HangfireDbContext connection)
        {
            return new LiteDbJobQueueMonitoringApi(connection);
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            var connection = ConnectionUtils.CreateConnection();
            action(connection);
        }
    }
#pragma warning restore 1591
}

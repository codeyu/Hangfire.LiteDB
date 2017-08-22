using System;
using System.Linq;
using Hangfire.LiteDB.Entities;
using Hangfire.LiteDB.Test.Utils;
using LiteDB;
using Xunit;

namespace Hangfire.LiteDB.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDbFetchedJobFacts
    {
        private const string JobId = "id";
        private const string Queue = "queue";


        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new LiteDbFetchedJob(null, ObjectId.NewObjectId(), JobId, Queue));

                Assert.Equal("connection", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(() => new LiteDbFetchedJob(connection, ObjectId.NewObjectId(), null, Queue));

                Assert.Equal("jobId", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new LiteDbFetchedJob(connection, ObjectId.NewObjectId(), JobId, null));

                Assert.Equal("queue", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            UseConnection(connection =>
            {
                var fetchedJob = new LiteDbFetchedJob(connection, ObjectId.NewObjectId(), JobId, Queue);

                Assert.Equal(JobId, fetchedJob.JobId);
                Assert.Equal(Queue, fetchedJob.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            UseConnection(connection =>
            {
                // Arrange
                var queue = "default";
                var jobId = ObjectId.NewObjectId().ToString();
                var id = CreateJobQueueRecord(connection, jobId, queue);
                var processingJob = new LiteDbFetchedJob(connection, id, jobId, queue);

                // Act
                processingJob.RemoveFromQueue();

                // Assert
                var count = connection.JobQueue.Count();
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
        {
            UseConnection(connection =>
            {
                // Arrange
                CreateJobQueueRecord(connection, "1", "default");
                CreateJobQueueRecord(connection, "2", "critical");
                CreateJobQueueRecord(connection, "3", "default");

                var fetchedJob = new LiteDbFetchedJob(connection, ObjectId.NewObjectId(), "999", "default");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                var count = connection.JobQueue.Count();
                Assert.Equal(3, count);
            });
        }

        [Fact, CleanDatabase]
        public void Requeue_SetsFetchedAtValueToNull()
        {
            UseConnection(connection =>
            {
                // Arrange
                var queue = "default";
                var jobId = ObjectId.NewObjectId().ToString();
                var id = CreateJobQueueRecord(connection, jobId, queue);
                var processingJob = new LiteDbFetchedJob(connection, id, jobId, queue);

                // Act
                processingJob.Requeue();

                // Assert
                var record = connection.JobQueue.FindAll().ToList().Single();
                Assert.Null(record.FetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            UseConnection(connection =>
            {
                // Arrange
                var queue = "default";
                var jobId = ObjectId.NewObjectId().ToString();
                var id = CreateJobQueueRecord(connection, jobId, queue);
                var processingJob = new LiteDbFetchedJob(connection, id, jobId, queue);

                // Act
                processingJob.Dispose();

                // Assert
                var record = connection.JobQueue.FindAll().ToList().Single();
                Assert.Null(record.FetchedAt);
            });
        }

        private static ObjectId CreateJobQueueRecord(HangfireDbContext connection, string jobId, string queue)
        {
            var jobQueue = new JobQueue
            {
                Id = ObjectId.NewObjectId(),
                JobId = jobId,
                Queue = queue,
                FetchedAt = DateTime.UtcNow
            };

            connection.JobQueue.Insert(jobQueue);

            return jobQueue.Id;
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            var connection = ConnectionUtils.CreateConnection();
            action(connection);
        }
    }
#pragma warning restore 1591
}
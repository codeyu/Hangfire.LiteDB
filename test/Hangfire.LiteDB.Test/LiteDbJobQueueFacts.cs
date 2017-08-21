using System;
using System.Linq;
using System.Threading;
using Hangfire.LiteDB.Entities;
using Hangfire.LiteDB.Test.Utils;
using Xunit;

namespace Hangfire.LiteDB.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDbJobQueueFacts
    {
        private static readonly string[] DefaultQueues = { "default" };

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() =>
                new LiteDbJobQueue(null, new LiteDbStorageOptions()));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(() =>
                    new LiteDbJobQueue(connection, null));

                Assert.Equal("storageOptions", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentNullException>(() =>
                    queue.Dequeue(null, CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                var exception = Assert.Throws<ArgumentException>(() =>
                    queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact]
        public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            UseConnection(connection =>
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(() =>
                    queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            UseConnection(connection =>
            {
                var cts = new CancellationTokenSource(200);
                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(() =>
                    queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            // Arrange
            UseConnection(connection =>
            {
                var jobQueue = new JobQueue
                {
                    JobId = 1.ToString(),
                    Queue = "default"
                };

                connection.JobQueue.Insert(jobQueue);

                var queue = CreateJobQueue(connection);

                // Act
                LiteDbFetchedJob payload = (LiteDbFetchedJob)queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.Equal("1", payload.JobId);
                Assert.Equal("default", payload.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue()
        {
            // Arrange
            UseConnection(connection =>
            {
                var job = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                connection.Job.Insert(job);

                var jobQueue = new JobQueue
                {
                    JobId = job.Id,
                    Queue = "default"
                };
                connection.JobQueue.Insert(jobQueue);

                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotNull(payload);

                var fetchedAt = connection.JobQueue.Find(_ => _.JobId== payload.JobId).FirstOrDefault().FetchedAt;

                Assert.NotNull(fetchedAt);
                Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            // Arrange
            UseConnection(connection =>
            {
                var job = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                connection.Job.Insert(job);

                var jobQueue = new JobQueue
                {
                    JobId = job.Id,
                    Queue = "default",
                    FetchedAt = DateTime.UtcNow.AddDays(-1)
                };
                connection.JobQueue.Insert(jobQueue);

                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotEmpty(payload.JobId);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            // Arrange
            UseConnection(connection =>
            {
                var job1 = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                connection.Job.Insert(job1);

                var job2 = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                connection.Job.Insert(job2);

                connection.JobQueue.Insert(new JobQueue
                {
                    JobId = job1.Id,
                    Queue = "default"
                });

                connection.JobQueue.Insert(new JobQueue
                {
                    JobId = job2.Id,
                    Queue = "default"
                });

                var queue = CreateJobQueue(connection);

                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                var otherJobFetchedAt = connection.JobQueue.Find(_ => _.JobId!= payload.JobId).FirstOrDefault().FetchedAt;

                Assert.Null(otherJobFetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            UseConnection(connection =>
            {
                var job1 = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                connection.Job.Insert(job1);

                connection.JobQueue.Insert(new JobQueue
                {
                    JobId = job1.Id,
                    Queue = "critical"
                });


                var queue = CreateJobQueue(connection);

                Assert.Throws<OperationCanceledException>(() => queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken()));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueuesBasedOnQueuePriority()
        {
            UseConnection(connection =>
            {
                var criticalJob = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                connection.Job.Insert(criticalJob);

                var defaultJob = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                connection.Job.Insert(defaultJob);

                connection.JobQueue.Insert(new JobQueue
                {
                    JobId = defaultJob.Id,
                    Queue = "default"
                });

                connection.JobQueue.Insert(new JobQueue
                {
                    JobId = criticalJob.Id,
                    Queue = "critical"
                });

                var queue = CreateJobQueue(connection);

                var critical = (LiteDbFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(critical.JobId);
                Assert.Equal("critical", critical.Queue);

                var @default = (LiteDbFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(@default.JobId);
                Assert.Equal("default", @default.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Enqueue_AddsAJobToTheQueue()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection);

                queue.Enqueue("default", "1");

                var record = connection.JobQueue.FindAll().ToList().Single();
                Assert.Equal("1", record.JobId.ToString());
                Assert.Equal("default", record.Queue);
                Assert.Null(record.FetchedAt);
            });
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        private static LiteDbJobQueue CreateJobQueue(HangfireDbContext connection)
        {
            return new LiteDbJobQueue(connection, new LiteDbStorageOptions());
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
#pragma warning restore 1591
}
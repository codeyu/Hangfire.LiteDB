using System;
using System.Collections.Generic;
using System.Diagnostics;
using Hangfire.Common;
using Hangfire.LiteDB.Entities;
using Hangfire.LiteDB.Test.Utils;
using Hangfire.States;
using Hangfire.Storage;
using LiteDB;
using Moq;
using Xunit;

namespace Hangfire.LiteDB.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDbMonitoringApiFacts
    {
        private const string DefaultQueue = "default";
        private const string FetchedStateName = "Fetched";
        private const int From = 0;
        private const int PerPage = 5;
        private readonly Mock<IPersistentJobQueueMonitoringApi> _persistentJobQueueMonitoringApi;
        private readonly PersistentJobQueueProviderCollection _providers;

        public LiteDbMonitoringApiFacts()
        {
            var queue = new Mock<IPersistentJobQueue>();
            _persistentJobQueueMonitoringApi = new Mock<IPersistentJobQueueMonitoringApi>();

            var provider = new Mock<IPersistentJobQueueProvider>();
            provider.Setup(x => x.GetJobQueue(It.IsNotNull<HangfireDbContext>())).Returns(queue.Object);
            provider.Setup(x => x.GetJobQueueMonitoringApi(It.IsNotNull<HangfireDbContext>()))
                .Returns(_persistentJobQueueMonitoringApi.Object);

            _providers = new PersistentJobQueueProviderCollection(provider.Object);
        }

        [Fact, CleanDatabase]
        public void GetStatistics_ReturnsZero_WhenNoJobsExist()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var result = monitoringApi.GetStatistics();
                Assert.Equal(0, result.Enqueued);
                Assert.Equal(0, result.Failed);
                Assert.Equal(0, result.Processing);
                Assert.Equal(0, result.Scheduled);
            });
        }

        [Fact, CleanDatabase]
        public void GetStatistics_ReturnsExpectedCounts_WhenJobsExist()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                CreateJobInState(database, 1, EnqueuedState.StateName);
                CreateJobInState(database,2, EnqueuedState.StateName);
                CreateJobInState(database, 4, FailedState.StateName);
                CreateJobInState(database, 5, ProcessingState.StateName);
                CreateJobInState(database, 6, ScheduledState.StateName);
                CreateJobInState(database, 7, ScheduledState.StateName);

                var result = monitoringApi.GetStatistics();
                Assert.Equal(2, result.Enqueued);
                Assert.Equal(1, result.Failed);
                Assert.Equal(1, result.Processing);
                Assert.Equal(2, result.Scheduled);
            });
        }

        [Fact, CleanDatabase]
        public void JobDetails_ReturnsNull_WhenThereIsNoSuchJob()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var result = monitoringApi.JobDetails("547527");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void JobDetails_ReturnsResult_WhenJobExists()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var job1 = CreateJobInState(database, 1, EnqueuedState.StateName);

                var result = monitoringApi.JobDetails(job1.IdString);

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
                Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var jobIds = new List<int>();

                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsNotFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(database, 1, EnqueuedState.StateName);

                var jobIds = new List<int> { unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(1, resultList.Count);
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsEmpty_WhenOneJobExistsThatIsFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(database, 1, FetchedStateName);

                var jobIds = new List<int> { fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsUnfetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(database, 1, EnqueuedState.StateName);
                var unfetchedJob2 = CreateJobInState(database,2, EnqueuedState.StateName);
                var fetchedJob = CreateJobInState(database, 3, FetchedStateName);

                var jobIds = new List<int> { unfetchedJob.Id, unfetchedJob2.Id, fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetEnqueuedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(2, resultList.Count);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var jobIds = new List<int>();

                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(database, 1, FetchedStateName);

                var jobIds = new List<int> { fetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(1, resultList.Count);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsEmpty_WhenOneJobExistsThatIsNotFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(database, 1, EnqueuedState.StateName);

                var jobIds = new List<int> { unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }
        
        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsFetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(database, 1, FetchedStateName);
                var fetchedJob2 = CreateJobInState(database,2, FetchedStateName);
                var unfetchedJob = CreateJobInState(database, 3, EnqueuedState.StateName);

                var jobIds = new List<int> { fetchedJob.Id, fetchedJob2.Id, unfetchedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                    .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(2, resultList.Count);
            });
        }

        [Fact, CleanDatabase]
        public void ProcessingJobs_ReturnsProcessingJobsOnly_WhenMultipleJobsExistsInProcessingSucceededAndEnqueuedState()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var processingJob = CreateJobInState(database, 1, ProcessingState.StateName);

                var succeededJob = CreateJobInState(database,2, SucceededState.StateName, liteJob =>
                {
                    var processingState = new LiteState()
                    {
                        Name = ProcessingState.StateName,
                        Reason = null,
                        CreatedAt = DateTime.Now,
                        Data = new Dictionary<string, string>
                        {
                            ["ServerId"] = Guid.NewGuid().ToString(),
                            ["StartedAt"] =
                            JobHelper.SerializeDateTime(DateTime.Now.Subtract(TimeSpan.FromMilliseconds(500)))
                        }
                    };
                    var succeededState = liteJob.StateHistory[0];
                    liteJob.StateHistory = new List<LiteState> {processingState, succeededState};
                    return liteJob;
                });

                var enqueuedJob = CreateJobInState(database, 3, EnqueuedState.StateName);

                var jobIds = new List<int> { processingJob.Id, succeededJob.Id, enqueuedJob.Id };
                _persistentJobQueueMonitoringApi.Setup(x => x
                        .GetFetchedJobIds(DefaultQueue, From, PerPage))
                    .Returns(jobIds);

                var resultList = monitoringApi.ProcessingJobs(From, PerPage);

                Assert.Equal(1, resultList.Count);
            });
        }

        public static void SampleMethod(string arg)
        {
            Debug.WriteLine(arg);
        }

        private void UseMonitoringApi(Action<HangfireDbContext, LiteDbMonitoringApi> action)
        {
            var database = ConnectionUtils.CreateConnection();
            var connection = new LiteDbMonitoringApi(database, _providers);
            action(database, connection);
        }

        private LiteJob CreateJobInState(HangfireDbContext database, int jobId, string stateName, Func<LiteJob, LiteJob> visitor = null)
        {
            var job = Job.FromExpression(() => SampleMethod("wrong"));
            
            Dictionary<string, string> stateData;
            if (stateName == EnqueuedState.StateName)
            {
                stateData = new Dictionary<string, string> {["EnqueuedAt"] = $"{DateTime.Now:o}"};
            }
            else if (stateName == ProcessingState.StateName)
            {
                stateData = new Dictionary<string, string>
                {
                    ["ServerId"] = Guid.NewGuid().ToString(),
                    ["StartedAt"] = JobHelper.SerializeDateTime(DateTime.Now.Subtract(TimeSpan.FromMilliseconds(500)))
                };
            }
            else
            {
                stateData = new Dictionary<string, string>();
            }

            var jobState = new LiteState()
            {
                JobId = jobId,
                Name = stateName,
                Reason = null,
                CreatedAt = DateTime.Now,
                Data = stateData
            };

            var liteJob = new LiteJob
            {
                Id = jobId,
                InvocationData = JobHelper.ToJson(InvocationData.Serialize(job)),
                Arguments = "[\"\\\"Arguments\\\"\"]",
                StateName = stateName,
                CreatedAt = DateTime.Now,
                StateHistory = new List<LiteState>{jobState}
            };
            if (visitor != null)
            {
                liteJob = visitor(liteJob);
            }
            database.Job.Insert(liteJob);

            var jobQueueDto = new JobQueue
            {
                FetchedAt = null,
                JobId = jobId,
                Queue = DefaultQueue
            };

            if (stateName == FetchedStateName)
            {
                jobQueueDto.FetchedAt = DateTime.Now;
            }

            database.JobQueue.Insert(jobQueueDto);

            return liteJob;
        }
    }
#pragma warning restore 1591
}

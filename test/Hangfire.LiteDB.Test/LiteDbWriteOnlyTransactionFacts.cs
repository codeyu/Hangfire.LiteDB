using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.LiteDB.Entities;
using Hangfire.LiteDB.Test.Utils;
using Hangfire.States;
using Moq;
using Xunit;

namespace Hangfire.LiteDB.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class LiteDbWriteOnlyTransactionFacts
    {
        private readonly PersistentJobQueueProviderCollection _queueProviders;

        public LiteDbWriteOnlyTransactionFacts()
        {
            Mock<IPersistentJobQueueProvider> defaultProvider = new Mock<IPersistentJobQueueProvider>();
            defaultProvider.Setup(x => x.GetJobQueue(It.IsNotNull<HangfireDbContext>()))
                .Returns(new Mock<IPersistentJobQueue>().Object);

            _queueProviders = new PersistentJobQueueProviderCollection(defaultProvider.Object);
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfConnectionIsNull()
        {
            ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new LiteDbWriteOnlyTransaction(null, _queueProviders));

            Assert.Equal("connection", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfProvidersCollectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new LiteDbWriteOnlyTransaction(ConnectionUtils.CreateConnection(), null));

            Assert.Equal("queueProviders", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfLiteDBStorageOptionsIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new LiteDbWriteOnlyTransaction(ConnectionUtils.CreateConnection(), _queueProviders));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void ExpireJob_SetsJobExpirationData()
        {
            UseConnection(database =>
            {
                LiteJob job = new LiteJob
                {
                    
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.Now
                };
                database.Job.Insert(job);

                LiteJob anotherJob = new LiteJob
                {
                    
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.Now
                };
                database.Job.Insert(anotherJob);

                var jobId = job.Id.ToString();
                var anotherJobId = anotherJob.Id;

                Commit(database, x => x.ExpireJob(jobId, TimeSpan.FromDays(1)));

                var testJob = GetTestJob(database, job.Id);
                Assert.True(DateTime.Now.AddMinutes(-1) < testJob.ExpireAt && testJob.ExpireAt <= DateTime.Now.AddDays(1));

                var anotherTestJob = GetTestJob(database, anotherJobId);
                Assert.Null(anotherTestJob.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void PersistJob_ClearsTheJobExpirationData()
        {
            UseConnection(database =>
            {
                LiteJob job = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.Now,
                    ExpireAt = DateTime.Now
                };
                database.Job.Insert(job);

                LiteJob anotherJob = new LiteJob
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.Now,
                    ExpireAt = DateTime.Now
                };
                database.Job.Insert(anotherJob);

                var jobId = job.Id.ToString();
                var anotherJobId = anotherJob.Id;

                Commit(database, x => x.PersistJob(jobId));

                var testjob = GetTestJob(database, job.Id);
                Assert.Null(testjob.ExpireAt);

                var anotherTestJob = GetTestJob(database, anotherJobId);
                Assert.NotNull(anotherTestJob.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void SetJobState_AppendsAStateAndSetItToTheJob()
        {
            UseConnection(database =>
            {
                LiteJob job = new LiteJob
                {
                   
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.Now
                };
                database.Job.Insert(job);

                LiteJob anotherJob = new LiteJob
                {
                    
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.Now
                };
                database.Job.Insert(anotherJob);

                var jobId = job.Id.ToString();
	            var serializedData = new Dictionary<string, string> {{"Name", "Value"}};

				var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData()).Returns(serializedData);

                Commit(database, x => x.SetJobState(jobId, state.Object));

                var testJob = GetTestJob(database, job.Id);
                Assert.Equal("State", testJob.StateName);
                Assert.Equal(1, testJob.StateHistory.Length);

                var anotherTestJob = GetTestJob(database, anotherJob.Id);
                Assert.Null(anotherTestJob.StateName);
                Assert.Equal(0, anotherTestJob.StateHistory.Length);

                var jobWithStates = database.Job.FindAll().ToList().FirstOrDefault();
                
                var jobState = jobWithStates.StateHistory.Single();
                Assert.Equal("State", jobState.Name);
                Assert.Equal("Reason", jobState.Reason);
                Assert.NotNull(jobState.CreatedAt);
                Assert.Equal(serializedData, jobState.Data);
            });
        }

        [Fact, CleanDatabase]
        public void AddJobState_JustAddsANewRecordInATable()
        {
            UseConnection(database =>
            {
                LiteJob job = new LiteJob
                {
                   
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.Now
                };
                database.Job.Insert(job);

                var jobId = job.IdString;
	            var serializedData = new Dictionary<string, string> {{"Name", "Value"}};

				var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData()).Returns(serializedData);

                Commit(database, x => x.AddJobState(jobId, state.Object));

                var testJob = GetTestJob(database, job.Id);
                Assert.Null(testJob.StateName);

                var jobWithStates = database.Job.FindAll().ToList().Single();
                var jobState = jobWithStates.StateHistory.Last();
                Assert.Equal("State", jobState.Name);
                Assert.Equal("Reason", jobState.Reason);
                Assert.NotNull(jobState.CreatedAt);
                Assert.Equal(serializedData, jobState.Data);
            });
        }

        [Fact, CleanDatabase]
        public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
        {
            UseConnection(database =>
            {
                var correctJobQueue = new Mock<IPersistentJobQueue>();
                var correctProvider = new Mock<IPersistentJobQueueProvider>();
                correctProvider.Setup(x => x.GetJobQueue(It.IsNotNull<HangfireDbContext>()))
                    .Returns(correctJobQueue.Object);

                _queueProviders.Add(correctProvider.Object, new[] { "default" });

                Commit(database, x => x.AddToQueue("default", "1"));

                correctJobQueue.Verify(x => x.Enqueue("default", "1"));
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.IncrementCounter("my-key"));

                Counter record = database.StateDataCounter.FindAll().ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(1L, record.Value);
                Assert.Equal(null, record.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.IncrementCounter("my-key", TimeSpan.FromDays(1)));

                Counter record = database.StateDataCounter.FindAll().ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(1L, record.Value);
                Assert.NotNull(record.ExpireAt);

                var expireAt = record.ExpireAt.Value;

                Assert.True(DateTime.Now.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.Now.AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.IncrementCounter("my-key");
                    x.IncrementCounter("my-key");
                });

                var recordCount = database.StateDataCounter.Count();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.DecrementCounter("my-key"));

                Counter record = database.StateDataCounter.FindAll().ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(-1L, record.Value);
                Assert.Equal(null, record.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.DecrementCounter("my-key", TimeSpan.FromDays(1)));

                Counter record = database.StateDataCounter.FindAll().ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(-1L, record.Value);
                Assert.NotNull(record.ExpireAt);

                var expireAt = (DateTime)record.ExpireAt;

                Assert.True(DateTime.Now.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.Now.AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.DecrementCounter("my-key");
                    x.DecrementCounter("my-key");
                });

                var recordCount = database.StateDataCounter.Count();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_IfThereIsNo_SuchKeyAndValue()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.AddToSet("my-key", "my-value"));

                LiteSet record = database.StateDataSet.FindAll().ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
                Assert.Equal(0.0, record.Score, 2);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_WhenKeyIsExists_ButValuesAreDifferent()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "another-value");
                });

                var recordCount = database.StateDataSet.Count();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_DoesNotAddARecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value");
                });

                var recordCount = database.StateDataSet.Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_AddsARecordWithScore_WhenBothKeyAndValueAreNotExist()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.AddToSet("my-key", "my-value", 3.2));

                LiteSet record = database.StateDataSet.FindAll().ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
                Assert.Equal(3.2, record.Score, 3);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_UpdatesAScore_WhenBothKeyAndValueAreExist()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value", 3.2);
                });

                LiteSet record = database.StateDataSet.FindAll().ToList().Single();

                Assert.Equal(3.2, record.Score, 3);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_RemovesARecord_WithGivenKeyAndValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "my-value");
                });

                var recordCount = database.StateDataSet.Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameKey_AndDifferentValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "different-value");
                });

                var recordCount = database.StateDataSet.Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameValue_AndDifferentKey()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("different-key", "my-value");
                });

                var recordCount = database.StateDataSet.Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsARecord_WithGivenValues()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.InsertToList("my-key", "my-value"));

                LiteList record = database.StateDataList.FindAll().ToList().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsAnotherRecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                });

                var recordCount = database.StateDataList.Count();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_RemovesAllRecords_WithGivenKeyAndValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "my-value");
                });

                var recordCount = database.StateDataList.Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameKey_ButDifferentValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "different-value");
                });

                var recordCount = database.StateDataList.Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameValue_ButDifferentKey()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("different-key", "my-value");
                });

                var recordCount = database.StateDataList.Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_TrimsAList_ToASpecifiedRange()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.InsertToList("my-key", "3");
                    x.TrimList("my-key", 1, 2);
                });

                LiteList[] records = database.StateDataList.FindAll().ToArray();

                Assert.Equal(2, records.Length);
                Assert.Equal("1", records[0].Value);
                Assert.Equal("2", records[1].Value);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecordsToEnd_IfKeepAndingAt_GreaterThanMaxElementIndex()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = database.StateDataList.Count();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_WhenStartingFromValue_GreaterThanMaxElementIndex()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = database.StateDataList.Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_IfStartFromGreaterThanEndingAt()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 0);
                });

                var recordCount = database.StateDataList.Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecords_OnlyOfAGivenKey()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("another-key", 1, 0);
                });

                var recordCount = database.StateDataList.Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(database =>
            {
                ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(database, x => x.SetRangeInHash(null, new Dictionary<string, string>())));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(database, x => x.SetRangeInHash("some-hash", null)));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" }
                        }));

                var result = database.StateDataHash.Find(_ => _.Key== "some-hash").ToList()
                    .ToDictionary(x => x.Field, x => x.Value);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(database =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => Commit(database, x => x.RemoveHash(null)));
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_RemovesAllHashRecords()
        {
            UseConnection(database =>
            {
                // Arrange
                Commit(database, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                        {
                            { "Key1", "Value1" },
                            { "Key2", "Value2" }
                        }));

                // Act
                Commit(database, x => x.RemoveHash("some-hash"));

                // Assert
                var count = database.StateDataHash.Count();
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireSet_SetsSetExpirationData()
        {
            UseConnection(database =>
            {
                var set1 = new LiteSet { Key = "Set1", Value = "value1" };
                database.StateDataSet.Insert(set1);

                var set2 = new LiteSet { Key = "Set2", Value = "value2" };
                database.StateDataSet.Insert(set2);

                Commit(database, x => x.ExpireSet(set1.Key, TimeSpan.FromDays(1)));

                var testSet1 = GetTestSet(database, set1.Key).FirstOrDefault();
                Assert.True(DateTime.Now.AddMinutes(-1) < testSet1.ExpireAt && testSet1.ExpireAt <= DateTime.Now.AddDays(1));

                var testSet2 = GetTestSet(database, set2.Key).FirstOrDefault();
                Assert.NotNull(testSet2);
                Assert.Null(testSet2.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireList_SetsListExpirationData()
        {
            UseConnection(database =>
            {
                var list1 = new LiteList { Key = "List1", Value = "value1" };
                database.StateDataList.Insert(list1);

                var list2 = new LiteList { Key = "List2", Value = "value2" };
                database.StateDataList.Insert(list2);

                Commit(database, x => x.ExpireList(list1.Key, TimeSpan.FromDays(1)));

                var testList1 = GetTestList(database, list1.Key);
                Assert.True(DateTime.Now.AddMinutes(-1) < testList1.ExpireAt && testList1.ExpireAt <= DateTime.Now.AddDays(1));

                var testList2 = GetTestList(database, list2.Key);
                Assert.Null(testList2.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireHash_SetsHashExpirationData()
        {
            UseConnection(database =>
            {
                var hash1 = new LiteHash { Key = "Hash1", Value = "value1" };
                database.StateDataHash.Insert(hash1);

                var hash2 = new LiteHash { Key = "Hash2", Value = "value2" };
                database.StateDataHash.Insert(hash2);

                Commit(database, x => x.ExpireHash(hash1.Key, TimeSpan.FromDays(1)));

                var testHash1 = GetTestHash(database, hash1.Key);
                Assert.True(DateTime.Now.AddMinutes(-1) < testHash1.ExpireAt && testHash1.ExpireAt <= DateTime.Now.AddDays(1));

                var testHash2 = GetTestHash(database, hash2.Key);
                Assert.Null(testHash2.ExpireAt);
            });
        }


        [Fact, CleanDatabase]
        public void PersistSet_ClearsTheSetExpirationData()
        {
            UseConnection(database =>
            {
                var set1 = new LiteSet { Key = "Set1", Value = "value1", ExpireAt = DateTime.Now };
                database.StateDataSet.Insert(set1);

                var set2 = new LiteSet { Key = "Set2", Value = "value2", ExpireAt = DateTime.Now };
                database.StateDataSet.Insert(set2);

                Commit(database, x => x.PersistSet(set1.Key));

                var testSet1 = GetTestSet(database, set1.Key).First();
                Assert.Null(testSet1.ExpireAt);

                var testSet2 = GetTestSet(database, set2.Key).First();
                Assert.NotNull(testSet2.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void PersistList_ClearsTheListExpirationData()
        {
            UseConnection(database =>
            {
                var list1 = new LiteList { Key = "List1", Value = "value1", ExpireAt = DateTime.Now };
                database.StateDataList.Insert(list1);

                var list2 = new LiteList { Key = "List2", Value = "value2", ExpireAt = DateTime.Now };
                database.StateDataList.Insert(list2);

                Commit(database, x => x.PersistList(list1.Key));

                var testList1 = GetTestList(database, list1.Key);
                Assert.Null(testList1.ExpireAt);

                var testList2 = GetTestList(database, list2.Key);
                Assert.NotNull(testList2.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void PersistHash_ClearsTheHashExpirationData()
        {
            UseConnection(database =>
            {
                var hash1 = new LiteHash { Key = "Hash1", Value = "value1", ExpireAt = DateTime.Now };
                database.StateDataHash.Insert(hash1);

                var hash2 = new LiteHash { Key = "Hash2", Value = "value2", ExpireAt = DateTime.Now };
                database.StateDataHash.Insert(hash2);

                Commit(database, x => x.PersistHash(hash1.Key));

                var testHash1 = GetTestHash(database, hash1.Key);
                Assert.Null(testHash1.ExpireAt);

                var testHash2 = GetTestHash(database, hash2.Key);
                Assert.NotNull(testHash2.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_AddToExistingSetData()
        {
            UseConnection(database =>
            {
                var set1Val1 = new LiteSet { Key = "Set1", Value = "value1", ExpireAt = DateTime.Now };
                database.StateDataSet.Insert(set1Val1);

                var set1Val2 = new LiteSet { Key = "Set1", Value = "value2", ExpireAt = DateTime.Now };
                database.StateDataSet.Insert(set1Val2);

                var set2 = new LiteSet { Key = "Set2", Value = "value2", ExpireAt = DateTime.Now };
                database.StateDataSet.Insert(set2);

                var values = new[] { "test1", "test2", "test3" };
                Commit(database, x => x.AddRangeToSet(set1Val1.Key, values));

                var testSet1 = GetTestSet(database, set1Val1.Key);
                var valuesToTest = new List<string>(values) {"value1", "value2"};

                Assert.NotNull(testSet1);
                // verify all values are present in testSet1
                Assert.True(testSet1.Select(s => s.Value.ToString()).All(value => valuesToTest.Contains(value)));
                Assert.Equal(5, testSet1.Count);

                var testSet2 = GetTestSet(database, set2.Key);
                Assert.NotNull(testSet2);
                Assert.Equal(1, testSet2.Count);
            });
        }


        [Fact, CleanDatabase]
        public void RemoveSet_ClearsTheSetData()
        {
            UseConnection(database =>
            {
                var set1Val1 = new LiteSet { Key = "Set1", Value = "value1", ExpireAt = DateTime.Now };
                database.StateDataSet.Insert(set1Val1);

                var set1Val2 = new LiteSet { Key = "Set1", Value = "value2", ExpireAt = DateTime.Now };
                database.StateDataSet.Insert(set1Val2);

                var set2 = new LiteSet { Key = "Set2", Value = "value2", ExpireAt = DateTime.Now };
                database.StateDataSet.Insert(set2);

                Commit(database, x => x.RemoveSet(set1Val1.Key));

                var testSet1 = GetTestSet(database, set1Val1.Key);
                Assert.Equal(0, testSet1.Count);

                var testSet2 = GetTestSet(database, set2.Key);
                Assert.Equal(1, testSet2.Count);
            });
        }


        private static LiteJob GetTestJob(HangfireDbContext database, int jobId)
        {
            return database.Job.FindById(jobId);
        }

        private static IList<LiteSet> GetTestSet(HangfireDbContext database, string key)
        {
            return database.StateDataSet.Find(_ => _.Key==key).ToList();
        }

        private static dynamic GetTestList(HangfireDbContext database, string key)
        {
            return database.StateDataList.Find(_ => _.Key== key).FirstOrDefault();
        }

        private static dynamic GetTestHash(HangfireDbContext database, string key)
        {
            return database.StateDataHash.Find(_ => _.Key== key).FirstOrDefault();
        }

        private void UseConnection(Action<HangfireDbContext> action)
        {
            HangfireDbContext connection = ConnectionUtils.CreateConnection();
            action(connection);
        }

        private void Commit(HangfireDbContext connection, Action<LiteDbWriteOnlyTransaction> action)
        {
            using (LiteDbWriteOnlyTransaction transaction = new LiteDbWriteOnlyTransaction(connection, _queueProviders))
            {
                action(transaction);
                transaction.Commit();
            }
        }
    }
#pragma warning restore 1591
}
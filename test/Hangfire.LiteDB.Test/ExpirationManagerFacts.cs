using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.LiteDB.Entities;
using Hangfire.LiteDB.Test.Utils;
using LiteDB;
using Xunit;

namespace Hangfire.LiteDB.Test
{
#pragma warning disable 1591
    [Collection("Database")]
    public class ExpirationManagerFacts
    {
        private readonly LiteDbStorage _storage;

        private readonly CancellationToken _token;
        private static PersistentJobQueueProviderCollection _queueProviders;

        public ExpirationManagerFacts()
        {
            _storage = ConnectionUtils.CreateStorage();
            _queueProviders = _storage.QueueProviders;

            _token = new CancellationToken(true);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null));
        }

        [Fact, CleanDatabase]
        public void Execute_RemovesOutdatedRecords()
        {
            var connection = ConnectionUtils.CreateConnection();
            CreateExpirationEntries(connection, DateTime.UtcNow.AddMonths(-1));
            var manager = CreateManager();
            manager.Execute(_token);
            Assert.True(IsEntryExpired(connection));
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            var connection = ConnectionUtils.CreateConnection();
            CreateExpirationEntries(connection, null);
            var manager = CreateManager();
            manager.Execute(_token);
            Assert.False(IsEntryExpired(connection));
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            var connection = ConnectionUtils.CreateConnection();
            CreateExpirationEntries(connection, DateTime.Now.AddMonths(1));
            var manager = CreateManager();
            manager.Execute(_token);
            Assert.False(IsEntryExpired(connection));
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_CounterTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            connection.StateDataCounter.Insert(new Counter
            {
                Id = ObjectId.NewObjectId(),
                Key = "key",
                Value = 1L,
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            });
            var manager = CreateManager();
            manager.Execute(_token);
            var count = connection.StateDataCounter.Count();
            Assert.Equal(0, count);
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            connection.Job.Insert(new LiteJob
            {
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow,
                ExpireAt = DateTime.UtcNow.AddMonths(-1),
            });
            var manager = CreateManager();
            manager.Execute(_token);
            var count = connection.Job.Count();
            Assert.Equal(0, count);
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            connection.StateDataList.Insert(new LiteList
            {
                Id = ObjectId.NewObjectId(),
                Key = "key",
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            });
            var manager = CreateManager();
            manager.Execute(_token);
            var count = connection
                .StateDataList
                .Count();
            Assert.Equal(0, count);
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            connection.StateDataSet.Insert(new LiteSet
            {
                Id = ObjectId.NewObjectId(),
                Key = "key",
                Score = 0,
                Value = "",
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            });
            var manager = CreateManager();
            manager.Execute(_token);
            var count = connection
                .StateDataSet
                .Count();
            Assert.Equal(0, count);
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            connection.StateDataHash.Insert(new LiteHash
            {
                Id = ObjectId.NewObjectId(),
                Key = "key",
                Field = "field",
                Value = "",
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            });
            var manager = CreateManager();
            manager.Execute(_token);
            var count = connection
                .StateDataHash
                .Count();
            Assert.Equal(0, count);
        }


        [Fact, CleanDatabase]
        public void Execute_Processes_AggregatedCounterTable()
        {
            var connection = ConnectionUtils.CreateConnection();
            connection.StateDataAggregatedCounter.Insert(new AggregatedCounter
            {
                Key = "key",
                Value = 1,
                ExpireAt = DateTime.UtcNow.AddMonths(-1)
            });
            var manager = CreateManager();
            manager.Execute(_token);
            Assert.Equal(0, connection
                .StateDataCounter
                .Count());
        }



        private static void CreateExpirationEntries(HangfireDbContext connection, DateTime? expireAt)
        {
            Commit(connection, x => x.AddToSet("my-key", "my-value"));
            Commit(connection, x => x.AddToSet("my-key", "my-value1"));
            Commit(connection, x => x.SetRangeInHash("my-hash-key", new[] { new KeyValuePair<string, string>("key", "value"), new KeyValuePair<string, string>("key1", "value1") }));
            Commit(connection, x => x.AddRangeToSet("my-key", new[] { "my-value", "my-value1" }));

            if (expireAt.HasValue)
            {
                var expireIn = expireAt.Value - DateTime.Now;
                Commit(connection, x => x.ExpireHash("my-hash-key", expireIn));
                Commit(connection, x => x.ExpireSet("my-key", expireIn));
            }
        }

        private static bool IsEntryExpired(HangfireDbContext connection)
        {
            var count = connection
                .StateDataExpiringKeyValue
                .Count();

            return count == 0;
        }

        private ExpirationManager CreateManager()
        {
            return new ExpirationManager(_storage);
        }

        private static void Commit(HangfireDbContext connection, Action<LiteDbWriteOnlyTransaction> action)
        {
            using (LiteDbWriteOnlyTransaction transaction = new LiteDbWriteOnlyTransaction(connection, _queueProviders, new LiteDbStorageOptions()))
            {
                action(transaction);
                transaction.Commit();
            }
        }
    }
#pragma warning restore 1591
}
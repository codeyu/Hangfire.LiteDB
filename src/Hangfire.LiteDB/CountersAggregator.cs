using System;
using System.Linq;
using System.Threading;
using Hangfire.LiteDB.Entities;
using Hangfire.Logging;
using Hangfire.Server;
using LiteDB;

namespace Hangfire.LiteDB
{
    /// <summary>
    /// Represents Counter collection aggregator for Mongo database
    /// </summary>
    public class CountersAggregator : IBackgroundProcess
    {
        private static readonly ILog Logger = LogProvider.For<CountersAggregator>();

        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

        private readonly LiteDbStorage _storage;
        private readonly TimeSpan _interval;

        /// <summary>
        /// Constructs Counter collection aggregator
        /// </summary>
        /// <param name="storage">LiteDB storage</param>
        /// <param name="interval">Checking interval</param>
        public CountersAggregator(LiteDbStorage storage, TimeSpan interval)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _interval = interval;
        }

        /// <summary>
        /// Runs aggregator
        /// </summary>
        /// <param name="context">Background processing context</param>
        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        /// <summary>
        /// Runs aggregator
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public void Execute(CancellationToken cancellationToken)
        {
            Logger.DebugFormat("Aggregating records in 'Counter' table...");

            long removedCount;

            do
            {
                using (var storageConnection = (LiteDbConnection)_storage.GetConnection())
                {
                    var database = storageConnection.Database;

                    var recordsToAggregate = database
                        .StateDataCounter
                        .FindAll()
                        .Take(NumberOfRecordsInSinglePass)
                        .ToList();

                    var recordsToMerge = recordsToAggregate
                        .GroupBy(_ => _.Key).Select(_ => new
                        {
                            _.Key,
                            Value = _.Sum(x => (long)x.Value),
                            ExpireAt = _.Max(x => x.ExpireAt)
                        });

                    foreach (var item in recordsToMerge)
                    {
                        AggregatedCounter aggregatedItem = database
                            .StateDataAggregatedCounter
                            .Find(Query.EQ("Key", item.Key))
                            .FirstOrDefault();

                        if (aggregatedItem != null)
                        {
                            var aggregatedCounters = database.StateDataAggregatedCounter.Find(_ => _.Key == item.Key);
                            foreach (var counter in aggregatedCounters)
                            {
                                counter.Value = (long) counter.Value + item.Value;
                                counter.ExpireAt = item.ExpireAt > aggregatedItem.ExpireAt
                                    ? item.ExpireAt
                                    : aggregatedItem.ExpireAt;
                                database.StateDataAggregatedCounter.Update(counter);
                            }
                            
                        }
                        else
                        {
                            database
                                .StateDataAggregatedCounter
                                .Insert(new AggregatedCounter
                            {
                                Id = ObjectId.NewObjectId(),
                                Key = item.Key,
                                Value = item.Value,
                                ExpireAt = item.ExpireAt
                            });
                        }
                    }

                    removedCount = database
                        .StateDataCounter
                        .Delete(Query.In("Id", recordsToAggregate.Select(_ => _.Id).ToBsonValueEnumerable()));
                }

                if (removedCount >= NumberOfRecordsInSinglePass)
                {
                    cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (removedCount >= NumberOfRecordsInSinglePass);

            cancellationToken.WaitHandle.WaitOne(_interval);
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            return "LiteDB Counter Colleciton Aggregator";
        }
    }
}